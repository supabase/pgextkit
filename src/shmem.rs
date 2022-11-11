use crate::types::SyncMut;
use hash32::*;
use pgx::cstr_core::cstr;
use pgx::prelude::*;
use std::hash::Hasher;
use std::mem::MaybeUninit;
use std::pin::Pin;

extern "C" fn make_hashkey(key: *const std::ffi::c_void, _keysize: pg_sys::Size) -> u32 {
    use hash32::Hasher;
    let key = key as *const heapless::String<96>;
    let mut hasher = Murmur3Hasher::default();
    unsafe {
        hasher.write((*key).as_bytes());
    }

    hasher.finish32()
}

extern "C" fn compare(
    key1: *const std::ffi::c_void,
    key2: *const std::ffi::c_void,
    _keysize: pg_sys::Size,
) -> i32 {
    let key1 = key1 as *const heapless::String<96>;
    let key2 = key2 as *const heapless::String<96>;
    unsafe {
        if (&*key1) == (&*key2) {
            0
        } else {
            1
        }
    }
}

const MAX_ATTACHMENTS: i64 = 8192;

pub struct SharedDictionary {
    hashtable: *mut pg_sys::HTAB,
}

impl Default for SharedDictionary {
    fn default() -> Self {
        let mut info = MaybeUninit::<pg_sys::HASHCTL>::zeroed();
        let infop = info.as_mut_ptr();
        unsafe {
            (*infop).keysize = std::mem::size_of::<heapless::String<96>>();
            (*infop).entrysize = std::mem::size_of::<Entry<heapless::String<96>, ()>>();
            (*infop).hash = Some(make_hashkey);
            (*infop).match_ = Some(compare);
        }
        let mut info = unsafe { info.assume_init() };

        let addin_shmem_init_lock: *mut pg_sys::LWLock =
            unsafe { &mut (*pg_sys::MainLWLockArray.add(21)).lock };
        unsafe {
            pg_sys::LWLockAcquire(addin_shmem_init_lock, pg_sys::LWLockMode_LW_EXCLUSIVE);
        }

        let hashtable = unsafe {
            pg_sys::ShmemInitHash(
                cstr!("pgextkit_shared_dictionary").as_ptr(),
                MAX_ATTACHMENTS,
                MAX_ATTACHMENTS,
                &mut info,
                (pg_sys::HASH_ELEM | pg_sys::HASH_BLOBS | pg_sys::HASH_COMPARE) as i32,
            )
        };

        unsafe {
            pg_sys::LWLockRelease(addin_shmem_init_lock);
        }

        Self { hashtable }
    }
}

impl SharedDictionary {
    pub fn insert<T: Unpin>(&mut self, name: &str, value: Pin<&mut T>) {
        let lock = unsafe {
            &mut (*pg_sys::GetNamedLWLockTranche(cstr!("pgextkit_shared_dictionary").as_ptr())).lock
        };
        unsafe {
            pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode_LW_EXCLUSIVE);
        }
        let name = heapless::String::<96>::from(name);
        let mut found = false;
        let mut entry = unsafe {
            pg_sys::hash_search_with_hash_value(
                self.hashtable,
                &name as *const heapless::String<96> as *const _,
                make_hashkey(
                    &name as *const heapless::String<96> as *const _,
                    std::mem::size_of::<heapless::String<96>>(),
                ),
                pg_sys::HASHACTION_HASH_ENTER_NULL,
                &mut found,
            ) as *mut Entry<heapless::String<96>, T>
        };
        unsafe {
            pg_sys::LWLockRelease(lock);
        }
        if !found {
            unsafe {
                (*entry).value = value.get_mut() as *mut _;
            }
        }
    }

    fn internal_get<T>(&self, name: &str) -> (bool, *mut T) {
        let lock = unsafe {
            &mut (*pg_sys::GetNamedLWLockTranche(cstr!("pgextkit_shared_dictionary").as_ptr())).lock
        };
        unsafe {
            pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode_LW_SHARED);
        }
        let name = heapless::String::<96>::from(name);
        let mut found = false;
        let entry = unsafe {
            pg_sys::hash_search_with_hash_value(
                self.hashtable,
                &name as *const heapless::String<96> as *const _,
                make_hashkey(
                    &name as *const heapless::String<96> as *const _,
                    std::mem::size_of::<heapless::String<96>>(),
                ),
                pg_sys::HASHACTION_HASH_FIND,
                &mut found,
            ) as *const Entry<heapless::String<96>, T>
        };
        unsafe {
            pg_sys::LWLockRelease(lock);
        }
        (found, unsafe { (*entry).value })
    }

    pub fn get_mut<T: Unpin + SyncMut>(&self, name: &str) -> Option<Pin<&'static mut T>> {
        if let (true, value) = self.internal_get(name) {
            Some(Pin::new(unsafe { &mut *(value as *mut T) }))
        } else {
            None
        }
    }

    pub fn get<T: Unpin>(&self, name: &str) -> Option<Pin<&'static T>> {
        if let (true, value) = self.internal_get(name) {
            Some(Pin::new(unsafe { &*(value as *const T) }))
        } else {
            None
        }
    }

    #[cfg(feature = "extension")]
    pub(crate) fn estimate_size() -> usize {
        unsafe {
            pg_sys::hash_estimate_size(
                MAX_ATTACHMENTS,
                std::mem::size_of::<*const ()>() + std::mem::size_of::<heapless::String<96>>(),
            )
        }
    }
}

#[repr(C, packed)]
struct Entry<K, V> {
    key: K,
    value: *mut V,
}
