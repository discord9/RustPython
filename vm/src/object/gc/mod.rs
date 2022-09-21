mod trace;
/// sync version of cycle collector
/// #[cfg(not(feature = "threading"))]
mod collector_sync;
mod header;
mod gc_obj;

use header::GcHeader;
use collector_sync::CcSync;
use trace::{GcObjPtr, GcTrace};