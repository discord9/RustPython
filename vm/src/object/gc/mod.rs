/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
mod collector_sync;
mod header;
mod trace;

pub use collector_sync::{CcSync, GcResult, GLOBAL_COLLECTOR};
pub use header::{Color, GcHeader};
pub use trace::{GcObjPtr, GcStatus, GcTrace, TracerFn};

fn deadlock_handler() -> ! {
    error!("Dead lock!");
    panic!("Dead lock!");
}
