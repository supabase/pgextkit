use crate::types::SyncMut;
use once_cell::sync::OnceCell;
use pgx::pg_sys;
use std::ffi::{CStr, CString};
use std::fmt;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};

type TrancheId = std::ffi::c_int;

pub struct PgDynamicLwLock<T> {
    lock: OnceCell<(TrancheId, pg_sys::LWLock)>,
    data: T,
    name: &'static CStr,
}

unsafe impl<T> SyncMut for PgDynamicLwLock<T> {}

impl<T> fmt::Debug for PgDynamicLwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "PgInnerDynamicLwLock({})",
            self.name.to_string_lossy()
        ))
    }
}

impl<T> PgDynamicLwLock<T> {
    pub fn new(name: &str, data: T) -> Self {
        let name = unsafe {
            CStr::from_ptr(
                Box::leak(
                    CString::new(name)
                        .expect("CString::new failed")
                        .into_boxed_c_str(),
                )
                .as_ptr(),
            )
        };

        PgDynamicLwLock {
            data,
            name,
            lock: OnceCell::new(),
        }
    }

    fn get_lock(&self) -> &(TrancheId, pg_sys::LWLock) {
        self.lock.get_or_init(|| {
            let tranche_id = unsafe { pg_sys::LWLockNewTrancheId() };
            unsafe { pg_sys::LWLockRegisterTranche(tranche_id, self.name.as_ptr()) }
            let mut lock = MaybeUninit::<pg_sys::LWLock>::zeroed();
            unsafe { pg_sys::LWLockInitialize(lock.as_mut_ptr(), tranche_id) }
            (tranche_id, unsafe { lock.assume_init() })
        })
    }

    fn register(&self) -> *const pg_sys::LWLock {
        let (tranche_id, lock) = self.get_lock();
        unsafe { pg_sys::LWLockRegisterTranche(*tranche_id, self.name.as_ptr()) }
        lock as *const _
    }

    /// Obtain a shared lock (which comes with `&T` access)
    pub fn share(&self) -> PgDynamicLwLockShareGuard<T> {
        let lock = self.register();
        unsafe {
            pg_sys::LWLockAcquire(lock as *mut _, pg_sys::LWLockMode_LW_SHARED);

            PgDynamicLwLockShareGuard {
                data: &self.data,
                lock: lock as *mut _,
            }
        }
    }

    pub fn exclusive(&mut self) -> PgDynamicLwLockExclusiveGuard<T> {
        let lock = self.register();
        unsafe {
            pg_sys::LWLockAcquire(lock as *mut _, pg_sys::LWLockMode_LW_EXCLUSIVE);

            PgDynamicLwLockExclusiveGuard {
                data: &mut self.data,
                lock: lock as *mut _,
            }
        }
    }
}

pub struct PgDynamicLwLockShareGuard<'a, T> {
    data: &'a T,
    lock: *mut pg_sys::LWLock,
}

impl<T> Drop for PgDynamicLwLockShareGuard<'_, T> {
    fn drop(&mut self) {
        unsafe {
            pg_sys::LWLockRelease(self.lock);
        }
    }
}

impl<T> Deref for PgDynamicLwLockShareGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.data
    }
}

pub struct PgDynamicLwLockExclusiveGuard<'a, T> {
    data: &'a mut T,
    lock: *mut pg_sys::LWLock,
}

impl<T> Deref for PgDynamicLwLockExclusiveGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.data
    }
}

impl<T> DerefMut for PgDynamicLwLockExclusiveGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.data
    }
}

impl<T> Drop for PgDynamicLwLockExclusiveGuard<'_, T> {
    fn drop(&mut self) {
        unsafe {
            pg_sys::LWLockRelease(self.lock);
        }
    }
}
