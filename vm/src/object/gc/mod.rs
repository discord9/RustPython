mod collector;
mod header;
mod trace;

pub use collector::{Collector, GLOBAL_COLLECTOR};
pub use header::{Color, GcHeader, GcResult};
pub use trace::{Trace, TraceHelper, TracerFn};

use crate::PyObject;

type GcObj = PyObject;
type GcObjRef<'a> = &'a GcObj;

#[derive(PartialEq, Eq)]
pub enum GcStatus {
    /// should be drop by caller
    ShouldDrop,
    /// because object is part of a garbage cycle, we don't want double dealloc
    /// or use after drop, so run `__del__` only. Drop(destructor)&dealloc is handle by gc
    GarbageCycle,
    /// already buffered, will be dealloc by collector, caller should call [`PyObject::del_Drop`] to run destructor only but not dealloc memory region
    BufferedDrop,
    /// should keep and not drop by caller
    ShouldKeep,
    /// Do Nothing, perhaps because it is RAII's deeds
    DoNothing,
}

impl GcStatus {
    /// if ref cnt already dropped to zero, then can drop
    pub fn can_drop(&self) -> bool {
        let stat = self;
        *stat == GcStatus::ShouldDrop
            || *stat == GcStatus::BufferedDrop
            || *stat == GcStatus::GarbageCycle
    }
}

pub fn collect() -> GcResult {
    #[cfg(feature = "gc")]
    {
        #[cfg(feature = "threading")]
        {
            GLOBAL_COLLECTOR.force_gc()
        }
        #[cfg(not(feature = "threading"))]
        {
            GLOBAL_COLLECTOR.with(|v| v.force_gc())
        }
    }
    #[cfg(not(feature = "gc"))]
    {
        Default::default()
    }
}

pub fn try_gc() -> GcResult {
    #[cfg(feature = "gc")]
    {
        #[cfg(feature = "threading")]
        {
            GLOBAL_COLLECTOR.fast_try_gc()
        }
        #[cfg(not(feature = "threading"))]
        {
            GLOBAL_COLLECTOR.with(|v| v.fast_try_gc())
        }
    }
    #[cfg(not(feature = "gc"))]
    {
        Default::default()
    }
}

pub fn isenabled() -> bool {
    #[cfg(feature = "gc")]
    {
        #[cfg(feature = "threading")]
        {
            GLOBAL_COLLECTOR.is_enabled()
        }
        #[cfg(not(feature = "threading"))]
        {
            GLOBAL_COLLECTOR.with(|v| v.is_enabled())
        }
    }
    #[cfg(not(feature = "gc"))]
    {
        false
    }
}

pub fn enable() {
    #[cfg(feature = "gc")]
    {
        #[cfg(feature = "threading")]
        {
            GLOBAL_COLLECTOR.enable()
        }
        #[cfg(not(feature = "threading"))]
        {
            GLOBAL_COLLECTOR.with(|v| v.enable())
        }
    }
    #[cfg(not(feature = "gc"))]
    return;
}

pub fn disable() {
    #[cfg(feature = "gc")]
    {
        #[cfg(feature = "threading")]
        {
            GLOBAL_COLLECTOR.disable()
        }
        #[cfg(not(feature = "threading"))]
        {
            GLOBAL_COLLECTOR.with(|v| v.disable())
        }
    }
    #[cfg(not(feature = "gc"))]
    return;
}
