mod collector;
mod header;
mod trace;

pub use collector::{Collector, GLOBAL_COLLECTOR};
pub use header::{Color, GcHeader, GcResult};
pub use trace::{Trace, TraceHelper, TracerFn};

use crate::PyObject;

type GcObj = PyObject;
type GcObjRef<'a> = &'a GcObj;

pub enum GcStatus {
    /// should be drop by caller
    ShouldDrop,
    /// because object is part of cycle, we don't want double dealloc
    ShouldDropOnly,
    /// already buffered, will be dealloc by collector, caller should call `drop_only` to run destructor only but not dealloc memory region
    BufferedDrop,
    /// should keep and not drop by caller
    ShouldKeep,
    /// Do Nothing, perhaps because it is RAII's deeds
    DoNothing,
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
