/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
#[cfg(feature = "gc")]
mod collector_sync;
#[cfg(feature = "gc")]
mod header;
#[cfg(feature = "gc")]
mod trace;

use std::time::Duration;

use crate::PyObject;
#[cfg(feature = "gc")]
pub(crate) use collector_sync::{CcSync, GLOBAL_COLLECTOR};
#[cfg(feature = "gc")]
pub(crate) use header::{Color, GcHeader};
#[cfg(feature = "gc")]
pub(crate) use trace::{GcObjPtr, GcStatus, GcTrace, TracerFn};
type GcObj = PyObject;
type GcObjRef<'a> = &'a GcObj;

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

static LOCK_TIMEOUT: Duration = Duration::from_secs(10);

fn deadlock_handler() -> ! {
    error!("Dead lock!");
    panic!("Dead lock!");
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
            GLOBAL_COLLECTOR.with(|v| v.force_gc().into())
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
