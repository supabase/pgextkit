use crate::types::SyncMut;
use bitflags::bitflags;
use once_cell::sync::OnceCell;
use pgx::check_for_interrupts;
use pgx::prelude::*;
use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SharedLatch {
    latch: pg_sys::Latch,
}
unsafe impl Sync for SharedLatch {}

impl SharedLatch {
    pub fn new() -> Self {
        let mut latch = unsafe { MaybeUninit::<pg_sys::Latch>::zeroed().assume_init() };
        unsafe {
            pg_sys::InitSharedLatch(&mut latch);
        }
        Self { latch }
    }

    pub fn own(&mut self) -> Option<OwnedLatch> {
        unsafe { pg_sys::OwnLatch(&mut self.latch) }
        Some(OwnedLatch::new(&mut self.latch as *mut _))
    }

    pub fn set_and_wake_up(&mut self) {
        #[cfg(feature = "raw-set-latch")]
        extern "C" {
            fn SetLatch(latch: *mut pg_sys::Latch);
        }
        #[cfg(not(feature = "raw-set-latch"))]
        use pg_sys::SetLatch;
        unsafe { SetLatch(&mut self.latch as *mut _) }
    }
}

unsafe impl SyncMut for SharedLatch {}

pub struct OwnedLatch {
    latch: *mut pg_sys::Latch,
    rc: Arc<LatchPtr>,
}

bitflags! {
    /// Flags to indicate when a BackgroundWorker should be awaken
    pub struct SignalWakeFlags: i32 {
        const SIGHUP = 0x1;
        const SIGTERM = 0x2;
    }
}

struct LatchPtr(*mut pg_sys::Latch);
unsafe impl Send for LatchPtr {}
unsafe impl Sync for LatchPtr {}

static OWNED_LATCHES: OnceCell<Mutex<Vec<Weak<LatchPtr>>>> = OnceCell::new();
static SIGNALS: OnceCell<BTreeMap<SignalWakeFlags, AtomicBool>> = OnceCell::new();

impl OwnedLatch {
    fn new(latch: *mut pg_sys::Latch) -> Self {
        OWNED_LATCHES.get_or_init(|| Mutex::new(vec![]));
        SIGNALS.get_or_init(|| {
            let mut map = BTreeMap::new();
            map.insert(SignalWakeFlags::SIGTERM, AtomicBool::new(false));
            map.insert(SignalWakeFlags::SIGHUP, AtomicBool::new(false));
            map
        });
        Self {
            latch,
            rc: Arc::new(LatchPtr(latch)),
        }
    }

    fn wait_latch(&self, timeout: i64, wakeup_flags: u32) -> i32 {
        unsafe {
            let latch = pg_sys::WaitLatch(
                self.latch,
                wakeup_flags as _,
                timeout,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(self.latch);
            check_for_interrupts!();

            latch
        }
    }

    pub fn wait(&self, timeout: Option<Duration>) {
        match timeout {
            Some(t) => self.wait_latch(
                t.as_millis().try_into().unwrap(),
                pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH,
            ),
            None => self.wait_latch(0, pg_sys::WL_LATCH_SET | pg_sys::WL_POSTMASTER_DEATH),
        };
    }

    pub fn set_and_wake_up(&self) {
        unsafe { pg_sys::SetLatch(self.latch) }
    }

    pub fn disown(&self) {
        unsafe { pg_sys::DisownLatch(self.latch) }
    }

    pub fn attach_signal_handlers(&self, wake: SignalWakeFlags) {
        if let Some(latches) = OWNED_LATCHES.get() {
            latches
                .lock()
                .expect("can't lock latches")
                .push(Arc::downgrade(&self.rc));
        }
        if wake.contains(SignalWakeFlags::SIGHUP) {
            unsafe {
                pg_sys::pqsignal(pg_sys::SIGHUP as i32, Some(Self::signal_handler));
            }
        }
        if wake.contains(SignalWakeFlags::SIGTERM) {
            unsafe {
                pg_sys::pqsignal(pg_sys::SIGTERM as i32, Some(Self::signal_handler));
            }
        }
        unsafe {
            pg_sys::BackgroundWorkerUnblockSignals();
        }
    }

    extern "C" fn signal_handler(signal: i32) {
        if SignalWakeFlags::from_bits(signal)
            .unwrap_or_else(SignalWakeFlags::empty)
            .contains(SignalWakeFlags::SIGHUP)
        {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext_PGC_SIGHUP);
            }
        }
        if let Some(latches) = OWNED_LATCHES.get() {
            for latch in &*latches.lock().expect("can't lock latches") {
                if let Some(signals) = SIGNALS.get() {
                    if let Some(latch) = latch.upgrade() {
                        if let Some(flag) =
                            signals.get(&SignalWakeFlags::from_bits(signal).unwrap())
                        {
                            flag.store(true, Ordering::SeqCst);
                        }
                        unsafe { pg_sys::SetLatch(latch.0) }
                    }
                }
            }
        }
    }

    pub fn signal_received(&self, wake: SignalWakeFlags) -> bool {
        if let Some(signals) = SIGNALS.get() {
            if let Some(flag) = signals.get(&wake) {
                return flag.swap(false, Ordering::SeqCst);
            }
        }
        false
    }
}

impl Drop for OwnedLatch {
    fn drop(&mut self) {
        self.disown();
    }
}

unsafe impl Send for OwnedLatch {}
unsafe impl Sync for OwnedLatch {}

impl Default for SharedLatch {
    fn default() -> Self {
        Self::new()
    }
}
