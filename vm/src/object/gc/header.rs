use std::sync::atomic::Ordering;

use crate::object::gc::{Collector, GLOBAL_COLLECTOR};
#[cfg(not(feature = "threading"))]
use rustpython_common::atomic::Radium;
use rustpython_common::lock::PyMutexGuard;
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
    /// TODO(discord9): compact color(2bit)+in_cycle(1)+buffered(1)+is_drop(1)+is_dealloc(1)+is_leak(1)=7bit into one byte
    color: PyMutex<Color>,
    /// prevent RAII to drop&dealloc when in cycle where should be drop&NOT dealloc
    in_cycle: PyMutex<bool>,
    buffered: PyMutex<bool>,
    is_drop: PyMutex<bool>,
    /// check for soundness
    is_dealloc: PyMutex<bool>,
    is_leak: PyMutex<bool>,
    exclusive: PyMutex<()>,
    gc: PyRc<Collector>,
}

impl Default for GcHeader {
    fn default() -> Self {
        Self {
            ref_cnt: 1.into(),
            color: PyMutex::new(Color::Black),
            in_cycle: PyMutex::new(false),
            buffered: PyMutex::new(false),
            is_drop: PyMutex::new(false),
            is_dealloc: PyMutex::new(false),
            is_leak: PyMutex::new(false),
            exclusive: PyMutex::new(()),
            /// when threading, using a global GC
            #[cfg(feature = "threading")]
            gc: GLOBAL_COLLECTOR.clone(),
            /// when not threading, using a gc per thread
            #[cfg(not(feature = "threading"))]
            gc: GLOBAL_COLLECTOR.with(|v| v.clone()),
        }
    }
}

impl GcHeader {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get(&self) -> usize {
        self.ref_cnt.load(Ordering::Relaxed)
    }

    pub fn exclusive(&self) -> PyMutexGuard<()> {
        self.exclusive.lock()
    }

    pub fn gc(&self) -> PyRc<Collector> {
        self.gc.clone()
    }

    pub fn is_in_cycle(&self) -> bool {
        *self.in_cycle.lock()
    }

    pub fn set_in_cycle(&self, b: bool) {
        *self.in_cycle.lock() = b;
    }

    pub fn is_drop(&self) -> bool {
        *self.is_drop.lock()
    }

    pub fn set_drop(&self) {
        *self.is_drop.lock() = true
    }

    pub fn is_dealloc(&self) -> bool {
        #[cfg(feature = "threading")]
        {
            *self
                .is_dealloc
                .try_lock_for(std::time::Duration::from_secs(1))
                .expect("Dead lock happen when should not, probably already deallocated")
        }
        #[cfg(not(feature = "threading"))]
        {
            *self
                .is_dealloc
                .try_lock()
                .expect("Dead lock happen when should not, probably already deallocated")
        }
    }

    pub fn set_dealloc(&self) {
        *self.is_dealloc.lock() = true
    }

    pub(crate) fn check_set_drop_dealloc(&self) -> bool {
        let is_dealloc = self.is_dealloc();
        if is_dealloc {
            warn!("Call a function inside a already deallocated object!");
            return false;
        }
        if !self.is_drop() && !is_dealloc {
            self.set_drop();
            self.set_dealloc();
            true
        } else {
            false
        }
    }

    /// return true if can drop(also mark object as dropped)
    pub(crate) fn check_set_drop_only(&self) -> bool {
        let is_dealloc = self.is_dealloc();
        if is_dealloc {
            warn!("Call a function inside a already deallocated object.");
            return false;
        }
        if !self.is_drop() && !is_dealloc {
            self.set_drop();
            true
        } else {
            false
        }
    }

    /// return true if can dealloc(that is already drop)
    pub(crate) fn check_set_dealloc_only(&self) -> bool {
        let is_drop = self.is_drop.lock();
        let is_dealloc = self.is_dealloc();
        if !*is_drop {
            warn!("Try to dealloc a object that haven't drop.");
            return false;
        }
        if *is_drop && !is_dealloc {
            self.set_dealloc();
            true
        } else {
            false
        }
    }

    pub fn try_pausing(&self) -> Option<PyRwLockReadGuard<()>> {
        if self.is_dealloc() {
            warn!("Try to pausing a already deallocated object: {:?}", self);
            return None;
        }
        self.gc.try_pausing()
    }

    /// This function will block if is a garbage collect is happening
    pub fn do_pausing(&self) {
        if self.is_dealloc() {
            warn!("Try to pausing a already deallocated object: {:?}", self);
            return;
        }
        self.gc.do_pausing();
    }
    pub fn color(&self) -> Color {
        *self.color.lock()
    }
    pub fn set_color(&self, new_color: Color) {
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
        let ret = self
            .ref_cnt
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prev| {
                (prev != 0).then_some(prev + 1)
            })
            .is_ok();
        if ret {
            self.set_color(Color::Black)
        }
        ret
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
        if self.is_leaked() {
            // warn!("Try to leak a already leaked object!");
            return;
        }
        *self.is_leak.lock() = true;
        /*
        const BIT_MARKER: usize = (std::isize::MAX as usize) + 1;
        debug_assert_eq!(BIT_MARKER.count_ones(), 1);
        debug_assert_eq!(BIT_MARKER.leading_zeros(), 0);
        self.ref_cnt.fetch_add(BIT_MARKER, Ordering::Relaxed);
        */
    }

    pub fn is_leaked(&self) -> bool {
        // (self.ref_cnt.load(Ordering::Acquire) as isize) < 0
        *self.is_leak.lock()
    }
}

/// other color(Green, Red, Orange) in the paper is not in use for now, so remove them in this enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Color {
    /// In use
    Black,
    /// Possible member of cycle
    Gray,
    /// Member of garbage cycle
    White,
    /// Possible root of cycle
    Purple,
}

#[derive(Debug, Default)]
pub struct GcResult {
    acyclic_cnt: usize,
    cyclic_cnt: usize,
}

impl GcResult {
    fn new(tuple: (usize, usize)) -> Self {
        Self {
            acyclic_cnt: tuple.0,
            cyclic_cnt: tuple.1,
        }
    }
}

impl From<(usize, usize)> for GcResult {
    fn from(t: (usize, usize)) -> Self {
        Self::new(t)
    }
}

impl From<GcResult> for (usize, usize) {
    fn from(g: GcResult) -> Self {
        (g.acyclic_cnt, g.cyclic_cnt)
    }
}

impl From<GcResult> for usize {
    fn from(g: GcResult) -> Self {
        g.acyclic_cnt + g.cyclic_cnt
    }
}
