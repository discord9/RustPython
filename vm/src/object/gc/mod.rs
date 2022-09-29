/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
mod collector_sync;
mod header;
mod trace;
// FIXME(discord9): incorrect way of drop and dealloc cause heap corruprtion and access voialtion
pub use collector_sync::{CcSync, GLOBAL_COLLECTOR, IS_GC_THREAD};
pub use header::GcHeader;
pub use trace::{GcObjPtr, GcStatus, GcTrace, TracerFn};
