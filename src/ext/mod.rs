use super::Magic;
use crate::shmem::SharedDictionary;
use crate::{Handle, VERSION};
use good_memory_allocator::SpinLockedAllocator;
use pgx::bgworkers::BackgroundWorkerBuilder;
use pgx::cstr_core::{cstr, CStr, CString};
use pgx::pg_sys::{AccessShareLock, ExtensionRelationId, ScanDirection_ForwardScanDirection};
use pgx::prelude::*;
use pgx::{pg_sys, FromDatum, GucContext, GucRegistry, GucSetting, IntoDatum};
use std::collections::HashMap;
use std::convert::AsRef;
use std::fs::{DirEntry, File};
use std::io::{BufRead, BufReader};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use std::time::Duration;

mod workers;

pgx::pg_module_magic!();

// Ensure pgexkit is preloaded, otherwise it's not very useful
extension_sql!(
    r#"
DO $$
DECLARE
s TEXT;
BEGIN
SELECT name FROM pg_settings WHERE name = 'shared_preload_libraries' AND setting LIKE '%pgextkit%' INTO s;
IF count(s) = 0
  THEN RAISE EXCEPTION 'postgresql.conf must contain pgexitkit.so in shared_preload_libraries';
END IF;
END $$;
"#,
    name = "config_check",
    bootstrap
);

static mut ALLOC_CALLBACKS: Vec<(
    extern "C" fn(*mut std::ffi::c_void, *const std::ffi::c_void),
    usize,
    *const std::ffi::c_void,
)> = vec![];

static ALLOCATOR: SpinLockedAllocator = SpinLockedAllocator::empty();

static mut SHMEM_SIZE: usize = 0;

static SHMEM_SIZE_SETTING: GucSetting<Option<&str>> =
    GucSetting::<Option<&str>>::new(Some("16 MiB"));

static mut BACKGROUND_WORKERS: Vec<(String, String, Box<pg_sys::BackgroundWorker>)> = vec![];

/// Initialization (happens when pgextkit is being preloaded)
#[pg_guard]
pub extern "C" fn _PG_init() {
    // At this point, we don't know which extensions are installed, so we find all of them that
    // conform to pgexkit signature and load them speculatively.
    // At a later point, a background worker will be started and it will proceed with further initialization
    // if warranted.

    for (name, version, path) in extkit_extensions() {
        pgx::log!(
            "Preparing {}--{} at {}",
            name,
            version,
            path.to_string_lossy()
        );
        match unsafe { libloading::Library::new(path.as_os_str()) } {
            Err(err) => {
                pgx::warning!("Couldn't load {}: {}", path.to_string_lossy(), err);
            }
            Ok(lib) => {
                let init = unsafe {
                    lib.get::<unsafe extern "C" fn(handle: *const Handle)>(
                        cstr!("pgextkit_init").to_bytes_with_nul(),
                    )
                };
                match init {
                    Err(_err) => {
                        pgx::warning!(
                            "Can't find pgxextkit_init in {}, skipping loading",
                            path.to_string_lossy()
                        );
                    }
                    Ok(init) => {
                        let handle = Handle::make_static(
                            name,
                            version,
                            path.file_stem()
                                .expect("filename")
                                .to_str()
                                .expect("string"),
                        );
                        unsafe {
                            init(&handle);
                        }
                        pgx::log!("Loaded pgextkit library {}", path.to_string_lossy());
                    }
                }
            }
        }
    }

    GucRegistry::define_string_guc(
        "pgextkit.shmem_size",
        "Shared memory size for pgextkit extensions",
        "Shared memory size for pgextkit extensions",
        &SHMEM_SIZE_SETTING,
        GucContext::Postmaster,
    );

    let shmem_size = parse_size::parse_size(
        SHMEM_SIZE_SETTING
            .get()
            .unwrap_or_else(|| "16MiB".to_string()),
    )
    .unwrap_or_else(|e| {
        pgx::warning!(
            "Invalid pgextkit.shmem_size setting ({}), setting it to its default (16MB)",
            e
        );
        16 * 1024 * 1024
    });
    pgx::log!("pgextkit: Initializing with {} shmem", shmem_size);
    unsafe {
        SHMEM_SIZE = shmem_size as usize;
    }
    #[cfg(not(feature = "pg15"))]
    unsafe {
        pg_sys::RequestAddinShmemSpace(shmem_size as usize);
        pg_sys::RequestAddinShmemSpace(SharedDictionary::size());
        pg_sys::RequestNamedLWLockTranche(cstr!("pgextkit_shared_dictionary").as_ptr(), 1);
    }

    unsafe {
        #[cfg(feature = "pg15")]
        {
            static mut PREV_SHMEM_REQUEST_HOOK: Option<unsafe extern "C" fn()> = None;
            PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
            pg_sys::shmem_request_hook = Some(__pgx_private_request_shmem_hook);
            #[pg_guard]
            unsafe extern "C" fn __pgx_private_request_shmem_hook() {
                if let Some(i) = PREV_SHMEM_REQUEST_HOOK {
                    i();
                }
                pg_sys::RequestAddinShmemSpace(SHMEM_SIZE);
                pg_sys::RequestAddinShmemSpace(SharedDictionary::size());
                pg_sys::RequestNamedLWLockTranche(cstr!("pgextkit_shared_dictionary").as_ptr(), 1);

                for (_cb, size, _payload) in ALLOC_CALLBACKS.iter() {
                    pg_sys::RequestAddinShmemSpace(*size);
                }
            }
        }

        static mut PREV_SHMEM_STARTUP_HOOK: Option<unsafe extern "C" fn()> = None;
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(__pgx_private_shmem_hook);

        #[pg_guard]
        unsafe extern "C" fn __pgx_private_shmem_hook() {
            if let Some(i) = PREV_SHMEM_STARTUP_HOOK {
                i();
            }

            // Ensure shared dictionary exists
            let _ = SharedDictionary::default();
            let shm_name = cstr!("pgextkit_shmem");
            let addin_shmem_init_lock: *mut pg_sys::LWLock =
                &mut (*pg_sys::MainLWLockArray.add(21)).lock;
            pg_sys::LWLockAcquire(addin_shmem_init_lock, pg_sys::LWLockMode_LW_EXCLUSIVE);

            let mut found = false;
            let allocated_shmem =
                pg_sys::ShmemInitStruct(shm_name.as_ptr(), SHMEM_SIZE, &mut found) as usize;

            pg_sys::LWLockRelease(addin_shmem_init_lock);

            if !ALLOCATOR.was_initialized() {
                ALLOCATOR.init(allocated_shmem, SHMEM_SIZE);
            }

            for (cb, size, payload) in ALLOC_CALLBACKS.drain(..) {
                let shm_name = CString::new(uuid::Uuid::new_v4().to_string())
                    .expect("can't create allocation name");
                pg_sys::LWLockAcquire(addin_shmem_init_lock, pg_sys::LWLockMode_LW_EXCLUSIVE);

                let mut found = false;
                let shmem = pg_sys::ShmemInitStruct(shm_name.into_raw(), size, &mut found);

                pg_sys::LWLockRelease(addin_shmem_init_lock);

                cb(shmem, payload);
            }
        }
    }

    BackgroundWorkerBuilder::new("pgextkit_master")
        .set_function("master_worker")
        .set_library("pgextkit")
        .set_argument(0.into_datum())
        .enable_spi_access()
        .enable_shmem_access(None)
        .set_restart_time(Some(Duration::from_millis(0)))
        .load();
}

fn substitute_libdir(s: &str) -> String {
    let pkglib = unsafe { CStr::from_ptr(pg_sys::pkglib_path.as_ptr()) }.to_string_lossy();
    let pkglib_str = pkglib.as_ref();
    s.replace("$libdir", pkglib_str)
}

fn has_magic(path: &PathBuf) -> Result<bool, anyhow::Error> {
    let lib = unsafe { libloading::Library::new(path)? };
    let magic = unsafe {
        lib.get::<unsafe extern "C" fn() -> *const Magic>(
            cstr!("pgextkit_magic").to_bytes_with_nul(),
        )
    };

    Ok(magic
        .ok()
        .and_then(|magic_func| {
            let magic: &'static Magic = unsafe { &*magic_func() };
            if magic.magic_size == size_of::<Magic>() && magic.version == VERSION {
                Some(())
            } else {
                None
            }
        })
        .is_some())
}

fn extkit_extensions() -> impl IntoIterator<Item = (String, String, PathBuf)> {
    control_files()
        .filter_map(|e| parse_control_file(&e).ok())
        // Check for magic function
        .filter(|(_, _, ref path)| match has_magic(path) {
            Ok(has_magic) => has_magic,
            Err(_err) => false,
        })
}

fn control_files() -> impl Iterator<Item = DirEntry> {
    let mut dir: PathBuf = {
        let mut path: [std::os::raw::c_char; pg_sys::MAXPGPATH as usize] =
            [0; pg_sys::MAXPGPATH as usize];
        unsafe {
            pg_sys::get_share_path(&pg_sys::my_exec_path as *const _, &mut path as *mut _);
            CStr::from_ptr(&path as *const _)
                .to_string_lossy()
                .to_string()
                .into()
        }
    };

    dir.push("extension");

    std::fs::read_dir(dir).ok().into_iter().flat_map(|dir| {
        dir.into_iter()
            // Get a valid entry
            .filter_map(Result::ok)
            // Filter for .control files
            .filter_map(|entry| {
                if let Some(true) = entry
                    .path()
                    .extension()
                    .map(|s| s.to_string_lossy().as_ref() == "control")
                {
                    Some(entry)
                } else {
                    None
                }
            })
    })
}

fn parse_control_file(entry: &DirEntry) -> Result<(String, String, PathBuf), anyhow::Error> {
    let entry_path = entry.path();

    let f = File::open(&entry_path)?;
    let reader = BufReader::new(f);

    let mut config = HashMap::new();
    for line in reader.lines() {
        let line = line?.split('#').next().unwrap_or("").to_string();

        if let &[k, v] = line
            .split('=')
            .map(str::trim)
            .collect::<Vec<_>>()
            .as_slice()
        {
            config.insert(
                k.to_string(),
                v.trim_start_matches('\'')
                    .trim_end_matches('\'')
                    .to_string(),
            );
        }
    }

    let stem = entry_path.file_stem().ok_or_else(|| {
        anyhow::Error::msg("can't get file name stem")
            .context(entry_path.to_string_lossy().to_string())
    })?;

    let (name, version) = match stem
        .to_string_lossy()
        .split("--")
        .collect::<Vec<_>>()
        .as_slice()
    {
        [extname, version] => (extname.to_string(), version.to_string()),
        [extname] => (
            extname.to_string(),
            config
                .get("default_version")
                .ok_or_else(|| {
                    anyhow::Error::msg("can't get default_version")
                        .context(entry_path.to_string_lossy().to_string())
                })?
                .to_string(),
        ),
        _ => {
            return Err(anyhow::Error::msg("invalid control file name")
                .context(entry_path.to_string_lossy().to_string()))
        }
    };

    let mut path = substitute_libdir(
        config
            .get("module_pathname")
            .ok_or_else(|| anyhow::Error::msg("module_pathname not found in control file"))?
            .as_str(),
    );
    path.push_str(".so");

    Ok((name, version, PathBuf::from(path)))
}

fn find_matching_control_file(
    extname: &str,
    version: Option<&str>,
) -> Result<(String, String, PathBuf), anyhow::Error> {
    let mut matching = control_files()
        // Filter for matching extension
        .filter_map(|entry| {
            if let Some(true) = entry.path().file_stem().map(|s| {
                let s = s.to_string_lossy();
                if s == extname {
                    true
                } else if let Some(version) = version {
                    s.split("--").collect::<Vec<_>>().as_slice() == [extname, version]
                } else {
                    matches!(s.split("--").collect::<Vec<_>>().as_slice(), &[name, ..] if name == extname)
                }
            }) {
                Some(entry)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    // Sort by length (more specific versions will be earlier)
    matching.sort_by(|x, y| {
        x.file_name()
            .to_string_lossy()
            .len()
            .cmp(&y.file_name().to_string_lossy().len())
            .reverse()
    });

    if let Some(matching_control_file) = matching.first() {
        parse_control_file(matching_control_file)
    } else {
        Err(anyhow::Error::msg("can't find matching control file"))
    }
}

#[pg_extern]
fn load(extname: &str, version: default!(Option<&str>, NULL)) {
    if let Ok((name, version, path)) = find_matching_control_file(extname, version) {
        let handle = Handle::make_dynamic(
            name,
            version,
            Path::new(&path)
                .file_stem()
                .expect("filename")
                .to_str()
                .expect("string"),
        );

        if has_magic(&path).expect("error while validating extension") {
            match unsafe { libloading::Library::new(&path) } {
                Err(err) => {
                    pgx::error!("Couldn't load {}: {}", path.to_string_lossy(), err);
                }
                Ok(lib) => {
                    let init = unsafe {
                        lib.get::<unsafe extern "C" fn(handle: *const Handle)>(
                            cstr!("pgextkit_init").to_bytes_with_nul(),
                        )
                    };
                    match init {
                        Err(_err) => {
                            pgx::warning!(
                                "Can't find pgxextkit_init in {}, skipping loading",
                                path.to_string_lossy()
                            );
                        }
                        Ok(init) => {
                            unsafe {
                                init(&handle);
                            }
                            pgx::log!("Loaded pgextkit library {}", path.to_string_lossy());
                        }
                    }
                }
            }
        }
    } else {
        pgx::error!("Can't find matching control file");
    }
}

#[pg_extern]
fn unload(extname: &str, version: default!(Option<&str>, NULL)) {
    let version = match version {
        None => {
            if let Some((_, version, _)) = get_extensions()
                .into_iter()
                .find(|(name, _, _)| name == extname)
            {
                version
            } else {
                pgx::error!("{} extension not found", extname);
            }
        }
        Some(version) => {
            if get_extensions()
                .iter()
                .any(|(name_, version_, _username)| name_ == extname && version_ == version)
            {
                version.to_string()
            } else {
                pgx::error!("{} extension at version {} not found", extname, version);
            }
        }
    };
    if let Ok((_name, _version, path)) = find_matching_control_file(extname, Some(&version)) {
        if has_magic(&path).expect("error while validating extension") {
            match unsafe { libloading::Library::new(&path) } {
                Err(err) => {
                    pgx::error!("Couldn't load {}: {}", path.to_string_lossy(), err);
                }
                Ok(lib) => {
                    let deinit = unsafe {
                        lib.get::<unsafe extern "C" fn()>(
                            cstr!("pgextkit_deinit").to_bytes_with_nul(),
                        )
                    };
                    match deinit {
                        Err(_err) => {
                            // No deinitialization required
                        }
                        Ok(deinit) => {
                            unsafe {
                                deinit();
                            }
                            pgx::log!("Unloaded pgextkit library {}", path.to_string_lossy());
                        }
                    }
                }
            }
        }
    } else {
        pgx::error!("Can't find matching control file");
    }
}

mod static_handle {
    use crate::ext::{ALLOC_CALLBACKS, BACKGROUND_WORKERS};
    use crate::Handle;
    use pgx::pg_sys;

    pub(crate) extern "C" fn allocate_shmem(
        _handle: *const Handle,
        size: usize,
        cb: extern "C" fn(*mut std::ffi::c_void, *const std::ffi::c_void),
        payload: *const std::ffi::c_void,
    ) {
        unsafe {
            #[cfg(not(feature = "pg15"))]
            pg_sys::RequestAddinShmemSpace(size);
            ALLOC_CALLBACKS.push((cb, size, payload));
        }
    }

    pub(crate) extern "C" fn register_bgworker(
        handle: *const Handle,
        bgw: *mut pg_sys::BackgroundWorker,
    ) {
        unsafe {
            let handle = &*handle;
            BACKGROUND_WORKERS.push((
                handle.name.to_string(),
                handle.version.to_string(),
                Box::new(*bgw),
            ));
        }
    }
}

mod dynamic_handle {
    use crate::ext::ALLOCATOR;
    use crate::types::{RpgffiChar128, RpgffiChar96};
    use crate::Handle;
    use pgx::{direct_function_call, pg_sys, FromDatum};
    use std::alloc::{GlobalAlloc, Layout};
    use std::ffi::CStr;

    pub(crate) extern "C" fn allocate_shmem(
        _handle: *const Handle,
        size: usize,
        cb: extern "C" fn(*mut std::ffi::c_void, *const std::ffi::c_void),
        payload: *const std::ffi::c_void,
    ) {
        let alloc = unsafe {
            ALLOCATOR.alloc(
                Layout::from_size_align(size, std::mem::size_of::<usize>())
                    .expect("Invalid layout"),
            )
        };
        cb(alloc as *mut _, payload);
    }

    pub(crate) extern "C" fn register_bgworker(
        _handle: *const Handle,
        bgw: *mut pg_sys::BackgroundWorker,
    ) {
        unsafe {
            let database: &CStr = FromDatum::from_polymorphic_datum(
                direct_function_call(pg_sys::current_database, vec![]).unwrap(),
                false,
                0,
            )
            .unwrap();
            let username = CStr::from_ptr(pg_sys::GetUserNameFromId(pg_sys::GetUserId(), false));
            (*bgw).bgw_name = RpgffiChar96::from(
                CStr::from_ptr((*bgw).bgw_name.as_ptr())
                    .to_string_lossy()
                    .replace("{{DATABASE}}", database.to_string_lossy().as_ref())
                    .as_str(),
            )
            .0;
            (*bgw).bgw_extra = RpgffiChar128::from(
                format!(
                    "{}@{}",
                    username.to_string_lossy().as_ref(),
                    database.to_string_lossy().as_ref()
                )
                .as_str(),
            )
            .0;
            pg_sys::RegisterDynamicBackgroundWorker(bgw, std::ptr::null_mut());
        }
    }
}
impl Handle {
    fn make_static(name: String, version: String, library_name: &str) -> Self {
        use static_handle::*;
        Self {
            allocate_shmem,
            register_bgworker,
            library_name: Box::leak(
                CString::new(library_name)
                    .expect("CString::new failed")
                    .into_boxed_c_str(),
            )
            .as_ptr(),
            name,
            version,
        }
    }

    fn make_dynamic(name: String, version: String, library_name: &str) -> Self {
        use dynamic_handle::*;
        Self {
            allocate_shmem,
            register_bgworker,
            library_name: Box::leak(
                CString::new(library_name)
                    .expect("CString::new failed")
                    .into_boxed_c_str(),
            )
            .as_ptr(),
            name,
            version,
        }
    }
}

fn get_extensions() -> Vec<(String, String, String)> {
    unsafe {
        let mut result = vec![];
        {
            let rel = pg_sys::table_open(ExtensionRelationId, AccessShareLock as _);
            let scan = pg_sys::table_beginscan_catalog(rel, 0, null_mut());
            loop {
                let tup = pg_sys::heap_getnext(scan, ScanDirection_ForwardScanDirection);
                if tup.is_null() {
                    break;
                }
                let version: String = String::from_polymorphic_datum(
                    pgx::heap_getattr_raw(tup, NonZeroUsize::new(6).unwrap(), (*rel).rd_att)
                        .unwrap(),
                    false,
                    0,
                )
                .unwrap();
                let ext = pg_sys::pgx_GETSTRUCT(tup) as pg_sys::Form_pg_extension;

                let str = CStr::from_ptr((*ext).extname.data.as_ptr());
                let name: String = str.to_string_lossy().into();

                let user_name: String =
                    CStr::from_ptr(pg_sys::GetUserNameFromId((*ext).extowner, true))
                        .to_string_lossy()
                        .into();

                result.push((name, version, user_name));
            }
            if let Some(end) = (*(*(*scan).rs_rd).rd_tableam).scan_end {
                end(scan);
            }
            pg_sys::table_close(rel, AccessShareLock as _);
        }
        result
    }
}

#[pg_extern]
fn shared_dictionary_entries(
) -> TableIterator<'static, (name!(name, String), name!(type_name, String))> {
    TableIterator::new(
        SharedDictionary::default()
            .entries()
            .map(|(name, type_name)| (name.to_string(), type_name.to_string()))
            .collect::<Vec<_>>()
            .into_iter(),
    )
}
