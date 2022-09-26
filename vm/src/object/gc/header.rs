use std::sync::atomic::Ordering;

use crate::object::gc::{CcSync, GLOBAL_COLLECTOR, IS_GC_THREAD};
#[cfg(not(feature = "threading"))]
use rustpython_common::atomic::Radium;
use rustpython_common::{
    atomic::PyAtomic,
    lock::{PyMutex, PyRwLockReadGuard},
    rc::PyRc,
};

/// Garbage collect header, containing ref count and other info, using repr(C) to stay consistent with PyInner 's repr
#[repr(C)]
#[derive(Debug)]
pub struct GcHeader {
    ref_cnt: PyAtomic<usize>,
    color: PyMutex<Color>,
    buffered: PyMutex<bool>,
    pub exclusive: PyMutex<()>,
    pub gc: PyRc<CcSync>,
}

impl GcHeader {
    pub fn new() -> Self {
        Self {
            ref_cnt: 1.into(),
            color: PyMutex::new(Color::Black),
            buffered: PyMutex::new(false),
            exclusive: PyMutex::new(()),
            #[cfg(feature = "threading")]
            gc: GLOBAL_COLLECTOR.clone(),
            #[cfg(not(feature = "threading"))]
            gc: GLOBAL_COLLECTOR.with(|v| v.clone()),
        }
    }

    pub fn try_pausing(&self) -> Option<PyRwLockReadGuard<()>> {
        if IS_GC_THREAD.with(|v| v.get()) {
            // if is same thread, then this thread is already stop by gc itself,
            // no need to block.
            // and any call to do_pausing is probably from drop() or what so allow it to continue execute.
            return None;
        }
        Some(self.gc.pause.read())
    }

    /// This function will block if is pausing by gc
    pub fn do_pausing(&self) {
        if IS_GC_THREAD.with(|v| v.get()) {
            // if is same thread, then this thread is already stop by gc itself,
            // no need to block.
            // and any call to do_pausing is probably from drop() or what so allow it to continue execute.
            return;
        }
        let _lock = self.gc.pause.read();
    }
    pub fn color(&self) -> Color {
        *self.color.lock()
    }
    pub fn set_color(&self, new_color: Color) {
        // dbg!(new_color);
        *self.color.lock() = new_color;
    }
    pub fn buffered(&self) -> bool {
        *self.buffered.lock()
    }
    pub fn set_buffered(&self, buffered: bool) {
        *self.buffered.lock() = buffered;
    }
    /// simple RC += 1
    pub fn inc(&self) -> usize {
        self.ref_cnt.fetch_add(1, Ordering::Relaxed) + 1
    }
    /// only inc if non-zero(and return true if success)
    #[inline]
    pub fn safe_inc(&self) -> bool {
        self.ref_cnt
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prev| {
                (prev != 0).then_some(prev + 1)
            })
            .is_ok()
    }
    /// simple RC -= 1
    pub fn dec(&self) -> usize {
        self.ref_cnt.fetch_sub(1, Ordering::Relaxed) - 1
    }
    pub fn rc(&self) -> usize {
        self.ref_cnt.load(Ordering::Relaxed)
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

/// other color(Green, Red, Orange) in the paper is not in use for now, so remove them in this enum
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
}
