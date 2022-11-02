use pgx::pg_sys;

use std::mem::size_of;

#[cfg(not(feature = "extension"))]
pub mod db;
#[cfg(feature = "extension")]
mod ext;
pub mod latch;
#[cfg(not(feature = "extension"))]
pub mod lwlock;
pub mod shmem;

pub mod types;

#[cfg(not(feature = "extension"))]
use crate::shmem::SharedDictionary;

#[cfg(not(feature = "extension"))]
pub mod prelude {
    pub use crate::db::*;
    pub use crate::latch::*;
    pub use crate::lwlock::*;
    pub use crate::shmem::*;
    pub use crate::types::*;
}

/// This structure is used to check whether an extension is of compatible version
#[repr(C)]
pub struct Magic {
    /// Size of the structure (size_of::<Magic>)
    magic_size: usize,
    /// Version of pgextkit supported (0)
    version: u8,
}

pub const VERSION: u8 = 0;

impl Magic {
    pub const fn new() -> Self {
        Self {
            magic_size: size_of::<Self>(),
            version: VERSION,
        }
    }
}

#[repr(C)]
pub struct Handle {
    allocate_shmem: extern "C" fn(
        handle: *const Handle,
        size: usize,
        cb: extern "C" fn(*mut std::ffi::c_void, *const std::ffi::c_void),
        payload: *const std::ffi::c_void,
    ),
    register_bgworker: extern "C" fn(handle: *const Handle, bgw: *mut pg_sys::BackgroundWorker),
    library_name: *const std::ffi::c_char,
    name: String,
    version: String,
}

#[no_mangle]
extern "C" fn allocate_shmem(
    handle: *const Handle,
    size: usize,
    cb: extern "C" fn(*mut std::ffi::c_void, *const std::ffi::c_void),
    payload: *const std::ffi::c_void,
) {
    unsafe { ((*handle).allocate_shmem)(handle, size, cb, payload) }
}

#[no_mangle]
extern "C" fn register_bgworker(handle: *const Handle, bgw: *mut pg_sys::BackgroundWorker) {
    unsafe { ((*handle).register_bgworker)(handle, bgw) }
}

#[cfg(not(feature = "extension"))]
use std::{borrow::Cow, ffi::CStr};

#[cfg(not(feature = "extension"))]
impl Handle {
    extern "C" fn call_closure<T, F: FnOnce(*mut T)>(
        mem: *mut std::ffi::c_void,
        payload: *const std::ffi::c_void,
    ) {
        let mem = unsafe { std::mem::transmute::<_, *mut T>(mem) };
        unsafe { Box::<F>::from_raw(payload as *mut _)(mem) }
    }

    pub fn allocate_shmem<T, F: FnOnce(*mut T)>(&self, f: F) {
        let ptr = Box::leak(Box::new(f)) as *mut F as *mut _;
        (self.allocate_shmem)(self, size_of::<T>(), Self::call_closure::<T, F>, ptr)
    }

    pub fn allocate_shmem_with<T: Unpin, F: FnOnce() -> T>(&self, name: &str, f: F) {
        use std::mem::ManuallyDrop;
        use std::pin::Pin;
        // We need to move this name so it stays allocated
        let name = String::from(name);
        self.allocate_shmem(move |mem| unsafe {
            *mem = ManuallyDrop::new(f());
            SharedDictionary::default().insert::<T>(name.as_str(), Pin::new(&mut *mem));
        });
    }

    pub fn allocate_shmem_for<T: Unpin>(&self, name: &str, val: T) {
        self.allocate_shmem_with(name, move || val)
    }

    pub fn register_bgworker<W: Into<pg_sys::BackgroundWorker>>(&self, worker: W) {
        let mut worker = worker.into();
        (self.register_bgworker)(self, &mut worker);
    }
    pub fn library_name<'a>(&'a self) -> Cow<'a, str> {
        unsafe { CStr::from_ptr(self.library_name).to_string_lossy() }
    }
}

#[macro_export]
macro_rules! pgextkit_magic {
    () => {
        #[no_mangle]
        #[allow(non_snake_case)]
        #[allow(unused)]
        #[link_name = "Pg_magic_func"]
        #[doc(hidden)]
        pub extern "C" fn pgextkit_magic() -> *const pgextkit::Magic {
            const MAGIC: pgextkit::Magic = pgextkit::Magic::new();
            &MAGIC
        }
    };
}

#[cfg(all(feature = "extension", any(test, feature = "pg_test")))]
#[pgx::pg_schema]
mod tests {}

#[cfg(all(feature = "extension", test))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
