/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
#[cfg(feature = "gc")]
mod collector_sync;
#[cfg(feature = "gc")]
mod header;
#[cfg(feature = "gc")]
mod trace;

use crate::PyObject;
#[cfg(feature = "gc")]
pub(crate) use collector_sync::{CcSync, GLOBAL_COLLECTOR};
#[cfg(feature = "gc")]
pub(crate) use header::{Color, GcHeader};
use rustpython_common::lock::{PyMutex, PyMutexGuard, PyRwLock, PyRwLockReadGuard};
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

#[cfg(feature = "threading")]
static LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(feature = "threading")]
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

/// try to lock it, if timeout, just ignore this lock's content in tracing for now
fn try_lock_timeout<T, F>(lock: &PyMutex<T>, f: F)
where
    F: FnOnce(PyMutexGuard<T>),
{
    #[cfg(feature = "threading")]
    match lock.try_lock() {
        Some(inner) => f(inner),
        None => {
            // that is likely a cause of someone else is holding a lock to inner field,
            // (like in multi-thread, another thread hold a lock(and also a Ref so no need to worry about it being drop) but then is stopped by gc)
            // but since the world is stopped, we can safely ignore this field?(Because it stay consistently inaccessible during `mark_gray`&`scan_black`)
            error!(
                "Could be in dead lock on type {}.",
                std::any::type_name::<T>()
            );
            #[cfg(debug_assertions)]
            {
                use backtrace::Backtrace;
                let bt = Backtrace::new();
                error!(
                    "Dead lock on {}: \n--------\n{:?}",
                    std::any::type_name::<T>(),
                    bt
                );
            }
            // stop to prevent heap corruption
            panic!();
        }
    }

    #[cfg(not(feature = "threading"))]
    match lock.try_lock() {
        Some(v) => f(v),
        None => {
            error!(
                "Could be in dead lock on type {}.",
                std::any::type_name::<T>()
            );
            #[cfg(debug_assertions)]
            {
                use backtrace::Backtrace;
                let bt = Backtrace::new();
                error!(
                    "Dead lock on {}: \n--------\n{:?}",
                    std::any::type_name::<T>(),
                    bt
                );
            }
        }
    }
}

fn try_read_timeout<T, F>(lock: &PyRwLock<T>, f: F)
where
    F: FnOnce(PyRwLockReadGuard<T>),
{
    #[cfg(feature = "threading")]
    match lock.try_read_recursive() {
        Some(inner) => f(inner),
        None => {
            // that is likely a cause of someone else is holding a lock to inner field,
            // but since the world is stopped, we can safely ignore this field?
            error!(
                "Could be in dead lock on type {}.",
                std::any::type_name::<T>()
            );
            #[cfg(debug_assertions)]
            {
                use backtrace::Backtrace;
                let bt = Backtrace::new();
                error!(
                    "Dead lock on {}: \n--------\n{:?}",
                    std::any::type_name::<T>(),
                    bt
                );
            }
        }
    }

    #[cfg(not(feature = "threading"))]
    match lock.try_read() {
        Some(v) => f(v),
        None => {
            error!(
                "Could be in dead lock on type {}.",
                std::any::type_name::<T>()
            );
            #[cfg(debug_assertions)]
            {
                use backtrace::Backtrace;
                let bt = Backtrace::new();
                error!(
                    "Dead lock on {}: \n--------\n{:?}",
                    std::any::type_name::<T>(),
                    bt
                );
            }
        }
    }
}
