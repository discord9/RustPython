use crate::common::{
    lock::{PyRwLock, PyRwLockReadGuard},
    rc::PyRc,
};
use crate::object::gc::collector::Collector;

/// `GcAction` return by calling `decrement()`,
/// which will tell the caller what to do next with current object
pub enum GcAction {
    /// full drop means run `__del__` and run destructor, then dealloc this object
    FullDrop,
    /// no dealloc means run `__del__` and run destructor, but **NOT** dealloc this object
    NoDealloc,
    /// only run `__del__`, **NOT** run `drop()` and **NOT** dealloc this object
    OnlyDel,
    /// do nothing, either because rc!=0 or rc already reach zero(because previous call to `decrement()`)
    /// or object is leaked and can't be drop
    Nothing,
}

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

    /// acquire a gc pausing lock, always success
    pub fn gc_pause(&self) -> GcPauseGuard {
        todo!()
    }
}

/// a lock to GC, prevent GC from happening
pub struct GcPauseGuard<'a>(Option<PyRwLockReadGuard<'a, ()>>);

impl GcPauseGuard<'_> {
    pub fn new() -> Self {
        todo!()
    }
}
