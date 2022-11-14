use pgextkit::prelude::*;
use pgx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder};
use pgx::prelude::*;

use std::fmt::Write;
use std::pin::Pin;

use std::time::Duration;

pgx::pg_module_magic!();
pgextkit::pgextkit_magic!();

extension_sql!(
    r#"
    SELECT pgextkit.load('example','0.0.0')
"#,
    name = "load",
    finalize
);

#[no_mangle]
fn pgextkit_init(handle: *mut pgextkit::Handle) {
    let handle = unsafe { &mut *handle } as &mut pgextkit::Handle;
    let worker = BackgroundWorkerBuilder::new("example ({{DATABASE}})")
        .set_library(&handle.library_name())
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_function("worker");
    handle.allocate_shmem_for(
        "LOCK",
        DatabaseLocal::<_, 8>::new(|| {
            PgDynamicLwLock::<heapless::String<96>>::new("A", "Test".into())
        }),
    );
    handle.allocate_shmem_for("LATCH", DatabaseLocal::<_, 8>::new(SharedLatch::new));
    handle.register_bgworker(&worker);
}

#[no_mangle]
fn pgextkit_deinit() {
    let dict = SharedDictionary::default();
    let lock: Pin<&mut DatabaseLocal<PgDynamicLwLock<heapless::String<96>>>> =
        dict.get_mut("LOCK").unwrap();
    let latch: Pin<&mut DatabaseLocal<SharedLatch>> = dict.get_mut("LATCH").unwrap();
    let mut latch = latch.for_my_database();

    let mut lock = lock.for_my_database();
    let mut s = lock.exclusive();
    s.clear();
    s.write_str("EXIT").unwrap();
    latch.set_and_wake_up();
}

#[no_mangle]
#[pg_guard]
extern "C" fn worker(_arg: pg_sys::Datum) {
    let dbinfo = BackgroundWorker::get_extra().split('@').collect::<Vec<_>>();
    assert!(dbinfo.len() == 2);
    let username = dbinfo[0];
    let database = dbinfo[1];
    BackgroundWorker::connect_worker_to_spi(Some(database), Some(username));

    pgx::log!("Starting worker on {} (user: {})", database, username);
    let dict = SharedDictionary::default();
    let lock: Pin<&mut DatabaseLocal<PgDynamicLwLock<heapless::String<96>>>> =
        dict.get_mut("LOCK").unwrap();
    let latch: Pin<&mut DatabaseLocal<SharedLatch>> = dict.get_mut("LATCH").unwrap();
    let mut latch = latch.for_my_database();

    let latch = latch.own().unwrap();
    let mut lock = lock.for_my_database();

    latch.attach_signal_handlers(SignalWakeFlags::SIGTERM);

    loop {
        {
            let guard = lock.share();
            let s = guard.as_str();
            if s == "EXIT" {
                drop(guard);
                lock.exclusive().clear();
                break;
            }
            pgx::log!("({}) {}", database, s);
        }
        latch.wait(Some(Duration::from_secs(10)));
        if latch.signal_received(SignalWakeFlags::SIGTERM) {
            break;
        }
    }
}

#[pg_extern]
fn hello_example(val: &str) {
    let dict = SharedDictionary::default();
    let lock: Pin<&mut DatabaseLocal<PgDynamicLwLock<heapless::String<96>>>> =
        dict.get_mut("LOCK").unwrap();
    let latch: Pin<&mut DatabaseLocal<SharedLatch>> = dict.get_mut("LATCH").unwrap();
    let mut latch = latch.for_my_database();

    let mut lock = lock.for_my_database();
    let mut s = lock.exclusive();
    s.clear();
    s.write_str(val).unwrap();
    latch.set_and_wake_up();
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::prelude::*;

    #[pg_test]
    fn test_hello_example() {
        assert_eq!("Hello, example", crate::hello_example());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
