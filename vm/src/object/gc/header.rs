use crate::common::{
    lock::{PyRwLock, PyRwLockWriteGuard},
    rc::PyRc,
};
use crate::object::gc::collector::Collector;

use super::collector::GLOBAL_COLLECTOR;

/// other color(Green, Red, Orange) in the paper is not in use for now, so remove them in this enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Color {
    /// In use(or free)
    Black,
    /// Possible member of cycle
    Gray,
    /// Member of garbage cycle
    White,
    /// Possible root of cycle
    Purple,
}

#[derive(Debug)]
pub struct GcHeader {
    inner: PyRwLock<GcHeaderInner>,
}

// mimic RefCnt's API
impl GcHeader {
    pub fn new() -> Self {
        Self {
            inner: PyRwLock::new(GcHeaderInner::new()),
        }
    }
    pub fn header(&self) -> PyRwLockWriteGuard<GcHeaderInner> {
        self.inner.write()
    }
    pub fn get(&self) -> usize {
        self.header().rc()
    }

    /// This is to mimic `RefCount::inc`
    pub fn inc(&self) {
        self.header().inc_black()
    }

    /// Returns true if successful
    pub fn safe_inc(&self) -> bool {
        let mut inner = self.header();
        if inner.rc() == 0 {
            return false;
        } else {
            inner.inc_black();
            return true;
        }
    }

    /// raw dec ref cnt, no shenanigans of adding buffer
    /// useful after trying to run `__del__` while keeping the object alive
    pub fn dec(&self) -> bool {
        self.header().dec() == 0
    }

    pub fn leak(&self) {
        self.header().set_leaked(true);
    }

    pub fn is_leaked(&self) -> bool {
        self.header().is_leaked()
    }
}

/// Garbage collect header, containing ref count and other info
/// During Garbage Collection, no concurrent access occured, so accessing this is ok,
/// but during normal operation, a mutex is needed hence `GcHeader`
#[derive(Debug)]
pub struct GcHeaderInner {
    ref_cnt: usize,
    color: Color,
    buffered: bool,
    leak: bool,
    /// this is for graceful dealloc object in cycle
    in_cycle: bool,

    gc: PyRc<Collector>,
}

impl GcHeaderInner {
    pub fn gc(&self) -> PyRc<Collector> {
        self.gc.clone()
    }
    pub fn new() -> Self {
        Self {
            ref_cnt: 1,
            color: Color::Black,
            buffered: false,
            leak: false,
            in_cycle: false,
            gc: GLOBAL_COLLECTOR.clone(),
        }
    }
    pub fn increment(&mut self) {
        self.inc_black()
    }
    /// inc ref cnt and set color to black, do nothing if leaked
    pub fn inc_black(&mut self) {
        if self.leak {
            return;
        }

        self.ref_cnt += 1;
        self.color = Color::Black;
    }

    pub fn inc(&mut self) {
        self.ref_cnt += 1;
    }

    pub fn dec(&mut self) -> usize {
        self.ref_cnt -= 1;
        self.ref_cnt
    }

    pub fn rc(&self) -> usize {
        self.ref_cnt
    }

    pub fn color(&self) -> Color {
        self.color
    }

    pub fn set_color(&mut self, new_color: Color) {
        self.color = new_color;
    }

    pub fn buffered(&self) -> bool {
        self.buffered
    }

    pub fn set_buffered(&mut self, buffered: bool) {
        self.buffered = buffered;
    }

    pub fn is_leaked(&self) -> bool {
        self.leak
    }

    pub fn set_leaked(&mut self, leaked: bool) {
        self.leak = leaked;
    }

    pub fn in_cycle(&self) -> bool {
        self.in_cycle
    }

    pub fn set_in_cycle(&mut self, in_cycle: bool) {
        self.in_cycle = in_cycle;
    }
}
