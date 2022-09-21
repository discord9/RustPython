mod trace;
/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
mod collector_sync;
mod header;
mod gc_obj;

pub use header::GcHeader;
pub use collector_sync::{CcSync, GLOBAL_COLLECTOR};
pub use trace::{GcObjPtr, GcTrace, TracerFn};