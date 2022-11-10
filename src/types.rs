use std::os::raw::c_char;

pub(crate) struct RpgffiChar128(pub(crate) [c_char; 128]);

impl<'a> From<&'a str> for RpgffiChar128 {
    fn from(string: &str) -> Self {
        let mut r = [0; 128];
        for (dest, src) in r.iter_mut().zip(string.as_bytes()) {
            *dest = *src as c_char;
        }
        RpgffiChar128(r)
    }
}

pub(crate) struct RpgffiChar96(pub(crate) [c_char; 96]);

impl<'a> From<&'a str> for RpgffiChar96 {
    fn from(string: &str) -> Self {
        let mut r = [0; 96];
        for (dest, src) in r.iter_mut().zip(string.as_bytes()) {
            *dest = *src as c_char;
        }
        RpgffiChar96(r)
    }
}

/// Marker that indicates that the type can be safely mutated across multiple threads of execution
///
/// # Safety
///
/// Unsafe if the type is not in fact safely mutable as intended.
pub unsafe trait SyncMut {}
