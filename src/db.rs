use crate::types::SyncMut;
use heapless::{Entry, FnvIndexMap, Vec};
use pgx::{pg_sys, pg_sys::Oid};
use pin_project::pin_project;
use std::pin::Pin;

#[pin_project]
pub struct DatabaseLocal<T: Unpin, const N: usize = 8> {
    #[pin]
    inner: Vec<T, N>,
    counter: usize,
    mapping: FnvIndexMap<Oid, usize, N>,
}

impl<T: Unpin, const N: usize> DatabaseLocal<T, N> {
    pub fn new<F: Fn() -> T>(f: F) -> Self {
        let inner = (0..N).into_iter().map(|_| f()).collect::<Vec<_, N>>();
        Self {
            inner,
            counter: 0,
            mapping: FnvIndexMap::new(),
        }
    }
    pub fn for_my_database(self: Pin<&mut Self>) -> Pin<&mut T> {
        let this = self.project();
        use pg_sys::MyDatabaseId;
        match this.mapping.entry(unsafe { MyDatabaseId }) {
            Entry::Vacant(entry) => {
                let _ = entry.insert(*this.counter);
                let result = Pin::new(this.inner.get_mut().get_mut(*this.counter).unwrap());
                *this.counter += 1;
                result
            }
            Entry::Occupied(entry) => Pin::new(this.inner.get_mut().get_mut(*entry.get()).unwrap()),
        }
    }
}

unsafe impl<T: Unpin, const N: usize> SyncMut for DatabaseLocal<T, N> {}
