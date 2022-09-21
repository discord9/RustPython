mod trace;
/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
mod collector_sync;
mod header;

pub use header::GcHeader;
pub use collector_sync::{CcSync, GLOBAL_COLLECTOR};
pub use trace::{GcObjPtr, GcTrace, TracerFn, GcStatus};