/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
mod collector_sync;
mod header;
mod trace;
use std::time::Duration;

pub use collector_sync::{CcSync, GcResult, GLOBAL_COLLECTOR};
pub use header::{Color, GcHeader};
pub use trace::{GcObjPtr, GcStatus, GcTrace, TracerFn};

static LOCK_TIMEOUT: Duration = Duration::from_secs(10);

fn deadlock_handler() -> ! {
    error!("Dead lock!");
    panic!("Dead lock!");
}
