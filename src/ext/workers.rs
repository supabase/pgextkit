use crate::ext;
use crate::ext::BACKGROUND_WORKERS;
use crate::types::RpgffiChar128;
use pgx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgx::cstr_core::CStr;
use pgx::pg_sys::{AccessShareLock, DatabaseRelationId, ScanDirection_ForwardScanDirection};
use pgx::{pg_guard, pg_sys, IntoDatum};
use std::collections::HashMap;
use std::ptr::null_mut;
use std::time::Duration;

#[pg_guard]
#[no_mangle]
pub extern "C" fn master_worker(_arg: pg_sys::Datum) {
    BackgroundWorker::connect_worker_to_spi(None, None);
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let mut databases = vec![];

    loop {
        let mut new_dbs = get_new_databases(databases.as_slice());
        if !new_dbs.is_empty() {
            for database in &new_dbs {
                let executor_startup = BackgroundWorkerBuilder::new(
                    format!("pgexitkit_database: {}", database).as_str(),
                )
                .set_function("database_worker")
                .set_library("pgextkit")
                .set_argument(0.into_datum())
                .set_extra(database)
                .set_restart_time(Some(Duration::from_secs(0)))
                .enable_spi_access()
                .enable_shmem_access(None)
                .set_notify_pid(unsafe { pg_sys::MyProcPid })
                .load_dynamic()
                .wait_for_startup();
                match executor_startup {
                    Ok(pid) => {
                        pgx::debug1!("Started pgextkit worker for `{}` (pid {})", database, pid);
                    }
                    Err(status) => {
                        pgx::error!(
                            "Failed to start pgextkit worker for `{}`: {:?}",
                            database,
                            status
                        );
                    }
                }
            }
            databases.append(&mut new_dbs);
        }
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(100))) {
            break;
        }
    }
}

fn get_new_databases(existing_databases: &[String]) -> Vec<String> {
    BackgroundWorker::transaction(|| unsafe {
        let mut result = vec![];
        {
            let rel = pg_sys::table_open(DatabaseRelationId, AccessShareLock as _);
            let scan = pg_sys::table_beginscan_catalog(rel, 0, null_mut());
            loop {
                let tup = pg_sys::heap_getnext(scan, ScanDirection_ForwardScanDirection);
                if tup.is_null() {
                    break;
                }
                let class = pg_sys::pgx_GETSTRUCT(tup) as pg_sys::Form_pg_database;
                if (*class).datistemplate || !(*class).datallowconn {
                    continue;
                }

                let str = CStr::from_ptr((*class).datname.data.as_ptr());
                let name: String = str.to_string_lossy().into();
                if !existing_databases.contains(&name) {
                    result.push(name);
                }
            }
            if let Some(end) = (*(*(*scan).rs_rd).rd_tableam).scan_end {
                end(scan);
            }
            pg_sys::table_close(rel, AccessShareLock as _);
        }
        result
    })
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn database_worker(_arg: pg_sys::Datum) {
    let database = BackgroundWorker::get_extra();
    BackgroundWorker::connect_worker_to_spi(Some(database), None);
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let extensions = BackgroundWorker::transaction(|| {
        ext::get_extensions()
            .into_iter()
            .map(|(name, version, username)| (name, (version, username)))
            .collect::<HashMap<_, _>>()
    });

    for (name, version, bgw) in unsafe { BACKGROUND_WORKERS.iter_mut() } {
        if let Some((installed_version, username)) = extensions.get(name) {
            if installed_version == version {
                unsafe {
                    bgw.bgw_extra =
                        RpgffiChar128::from(format!("{}@{}", username, database).as_str()).0;
                    pg_sys::RegisterDynamicBackgroundWorker(&mut **bgw, std::ptr::null_mut());
                }
            }
        }
    }

    loop {
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(100))) {
            break;
        }
    }
}
