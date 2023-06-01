use rustpython_common::{atomic::PyAtomic, lock::PyRwLockReadGuard};

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

/// Garbage collect header, containing ref count and other info
#[derive(Debug)]
pub struct GcHeader {
    ref_cnt: usize,
    color: Color,
    buffered: bool,
    leak: bool,
    /// this is for graceful dealloc object in cycle
    in_cycle: bool,
}

impl GcHeader {
    /// inc ref cnt and set color to black
    pub fn inc_black(&mut self) {
        if self.leak {
            return;
        }

        self.ref_cnt += 1;
        self.color = Color::Black;
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
