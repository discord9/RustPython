use std::sync::{
    atomic::Ordering,
    Mutex,
};

use rustpython_common::atomic::PyAtomic;
#[cfg(feature = "threading")]
pub struct GcHeader {
    ref_cnt: PyAtomic<usize>,
    color: Mutex<Color>,
    buffered: Mutex<bool>,
    // log_ptr: Mutex<Option<LogPointer>>,
}

#[cfg(not(feature = "threading"))]
pub struct GcHeader {
    ref_cnt: usize,
    color: core::cell::Cell<Color>,
    buffered: core::cell::Cell<bool>,
}

impl GcHeader {
    pub fn new() -> Self {
        Self {
            ref_cnt: 1.into(),
            color: Mutex::new(Color::Black),
            buffered: Mutex::new(false),
        }
    }
    pub fn color(&self) -> Color {
        *self.color.lock().unwrap()
    }
    pub fn set_color(&self, new_color: Color) {
        // dbg!(new_color);
        *self.color.lock().unwrap() = new_color;
    }
    pub fn buffered(&self) -> bool {
        *self.buffered.lock().unwrap()
    }
    pub  fn set_buffered(&self, buffered: bool) {
        *self.buffered.lock().unwrap() = buffered;
    }
    /// simple RC += 1
    pub fn inc(&self) -> usize {
        self.ref_cnt.fetch_add(1, Ordering::Relaxed) + 1
    }
    /// only inc if non-zero(and return true if success)
    #[inline]
    pub fn safe_inc(&self) -> bool {
        self.ref_cnt
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prev| (prev != 0).then(|| prev + 1))
            .is_ok()
    }
    /// simple RC -= 1
    pub fn dec(&self) -> usize {
        self.ref_cnt.fetch_sub(1, Ordering::Relaxed) - 1
    }
    pub fn rc(&self) -> usize {
        self.ref_cnt.load(Ordering::Relaxed)
    }
    pub fn get(&self) -> usize {
        self.rc()
    }
}

impl GcHeader {
    // move these functions out and give separated type once type range is stabilized

    pub fn leak(&self) {
        debug_assert!(!self.is_leaked());
        const BIT_MARKER: usize = (std::isize::MAX as usize) + 1;
        debug_assert_eq!(BIT_MARKER.count_ones(), 1);
        debug_assert_eq!(BIT_MARKER.leading_zeros(), 0);
        self.ref_cnt.fetch_add(BIT_MARKER, Ordering::Relaxed);
    }

    pub fn is_leaked(&self) -> bool {
        (self.ref_cnt.load(Ordering::Acquire) as isize) < 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Color {
    /// In use or free
    Black,
    /// Possible member of cycle
    Gray,
    /// Member of garbage cycle
    White,
    /// Possible root of cycle
    Purple,
    Green,
    Red,
    Orange,
}
