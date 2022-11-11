use crate::types::SyncMut;
use heapless::FnvIndexMap;
use pgx::cstr_core::cstr;
use pgx::prelude::*;
use std::pin::Pin;

const MAX_ATTACHMENTS: usize = 8192;

pub struct Entry {
    type_name: heapless::String<96>,
    ptr: *mut (),
}

pub type Map = FnvIndexMap<heapless::String<96>, Entry, MAX_ATTACHMENTS>;

pub struct SharedDictionary {
    map: *mut Map,
}

trait TruncatingFrom {
    fn truncating_from<S: AsRef<str>>(s: S) -> Self;
}

impl<const N: usize> TruncatingFrom for heapless::String<N> {
    fn truncating_from<S: AsRef<str>>(s: S) -> Self {
        Self::from(&s.as_ref()[0..N])
    }
}

impl Default for SharedDictionary {
    fn default() -> Self {
        let addin_shmem_init_lock: *mut pg_sys::LWLock =
            unsafe { &mut (*pg_sys::MainLWLockArray.add(21)).lock };
        unsafe {
            pg_sys::LWLockAcquire(addin_shmem_init_lock, pg_sys::LWLockMode_LW_EXCLUSIVE);
        }

        let mut found = false;
        let map = unsafe {
            pg_sys::ShmemInitStruct(
                cstr!("pgextkit_shared_dictionary").as_ptr(),
                Self::size(),
                &mut found as *mut _,
            )
        } as *mut _;

        if !found {
            unsafe {
                *map = FnvIndexMap::new();
            }
        }

        unsafe {
            pg_sys::LWLockRelease(addin_shmem_init_lock);
        }

        Self { map }
    }
}

impl SharedDictionary {
    pub fn insert<T: Unpin>(&mut self, name: &str, value: *mut T) {
        let lock = unsafe {
            &mut (*pg_sys::GetNamedLWLockTranche(cstr!("pgextkit_shared_dictionary").as_ptr())).lock
        };
        unsafe {
            pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode_LW_EXCLUSIVE);
        }
        let name = heapless::String::truncating_from(name);
        unsafe {
            let _ = (*self.map).insert(
                name,
                Entry {
                    type_name: heapless::String::truncating_from(std::any::type_name::<T>()),
                    ptr: value as *mut _,
                },
            );
        }
        unsafe {
            pg_sys::LWLockRelease(lock);
        }
    }

    fn internal_get<T>(&self, name: &str) -> Option<*mut T> {
        let lock = unsafe {
            &mut (*pg_sys::GetNamedLWLockTranche(cstr!("pgextkit_shared_dictionary").as_ptr())).lock
        };
        unsafe {
            pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode_LW_SHARED);
        }
        let name = heapless::String::truncating_from(name);
        let result = unsafe { (*self.map).get(&name) }.map(|entry| entry.ptr as *mut T);

        unsafe {
            pg_sys::LWLockRelease(lock);
        }

        result
    }

    pub fn get_mut<T: Unpin + SyncMut>(&self, name: &str) -> Option<Pin<&'static mut T>> {
        self.internal_get(name)
            .map(|ptr| Pin::new(unsafe { &mut *ptr }))
    }

    pub fn get<T: Unpin>(&self, name: &str) -> Option<Pin<&'static T>> {
        self.internal_get(name)
            .map(|ptr| Pin::new(unsafe { &*ptr }))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &str)> {
        unsafe {
            (*self.map)
                .iter()
                .map(|(name, entry)| (name.as_str(), entry.type_name.as_str()))
        }
    }

    pub fn size() -> usize {
        std::mem::size_of::<Map>()
    }
}
