/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
mod collector_sync;
mod header;
mod trace;

pub use collector_sync::GcResult;
pub(crate) use collector_sync::{CcSync, GLOBAL_COLLECTOR};
pub(crate) use header::{Color, GcHeader};
pub(crate) use trace::{GcObjPtr, GcStatus, GcTrace, TracerFn};
use crate::PyObject;
type GcObj = PyObject;
type GcObjRef<'a> = &'a GcObj;

fn deadlock_handler() -> ! {
    error!("Dead lock!");
    panic!("Dead lock!");
}

pub fn collect() -> GcResult {
    #[cfg(feature = "threading")]
    {
        GLOBAL_COLLECTOR.force_gc()
    }
    #[cfg(not(feature = "threading"))]
    {
        GLOBAL_COLLECTOR.with(|v| v.force_gc().into())
    }
}

pub fn isenabled() -> bool {
    #[cfg(feature = "threading")]
    {
        GLOBAL_COLLECTOR.is_enabled()
    }
    #[cfg(not(feature = "threading"))]
    {
        GLOBAL_COLLECTOR.with(|v| v.is_enabled())
    }
}

pub fn enable() {
    #[cfg(feature = "threading")]
    {
        GLOBAL_COLLECTOR.enable()
    }
    #[cfg(not(feature = "threading"))]
    {
        GLOBAL_COLLECTOR.with(|v| v.enable())
    }
}

pub fn disable() {
    #[cfg(feature = "threading")]
    {
        GLOBAL_COLLECTOR.disable()
    }
    #[cfg(not(feature = "threading"))]
    {
        GLOBAL_COLLECTOR.with(|v| v.disable())
    }
}
