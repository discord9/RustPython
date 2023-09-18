//! This is a simple stop-the-world coloring Garbage Collector implementation.
//! Here is the basic idea:
//! 1. We use a `Collector` to manage all the `GcObj`s.
//! 2. We use a `GcHeader` to manage the `GcObj`'s color and ref count.
//!
//! And the basic algorithm is from this paper: Concurrent Cycle Collection in Reference Counted Systems David F.Bacon and V.T.Rajan
//! the paper is here: https://dl.acm.org/doi/10.5555/646158.680003
//! So let me explain the algorithm a bit in my word:
//! Here I only implement the stop-the-world version of this algorithm, because concurrent version is a bit complex and require write barrier.
//! So the basic ideas here is:
//! 1. each object have three fields for GC, `buffered`(a bool), `color`(a enum), `ref_cnt`(a usize), the original paper have seven color,
//! but in our sync version there only need four color, which is the following:
//! | color | meaning |
//! | ----- | ------- |
//! | Black | In use or free |
//! | Gray  |Possible member of cycle |
//! | White | Member of garbage cycle |
//! | Purple| Possible root of cycle  |
//!
//! All objects start out black:
//! 1. when ref count is incremented, object is colored `Black`, since it is in use, it can not be garbage.
//! 2. When ref count is decremented, if it reach zero, it is released, And recursively decrement ref count on all its children.
//! else object is colored `Purple`, since it is considered to be a possible root of a garbage cycle(and buffered for delayed release).
//! 3. When releasing a object, first color it as `Black`(So later delayed release can know to free it) if it's NOT buffered, free it now, else reserve it for delayed release.
//! 4. Here comes the major Garbage Collection part, when certain condition is met(i.e. the root buffer is full or something else), we start a GC:
//! The GC is in three phrase: mark roots, scan roots and finally collect roots
//! 4.1. In mark roots phrase, we look at all object in root buffer, if it is `Purple` and still have non-zero
//! ref count, we call `MarkGray` to color it `Gray` and recursively mark all its children as `Gray`,
//! else it's pop out of buffer, and released if ref count is zero.
//! there we have a lot of possible member of cycle.
//! 4.2. Therefore we must found the real garbage cycle, hence the `ScanRoot` phrase,
//! where we call `Scan` for every remaining object in root buffer,
//! which will try and find live data in the cycle: if it finds a `Gray` object with ref count being non-zero,
//! the object itself and all its children are colored `Black` and this part cycle is considered to be live. This is done by call `ScanBlack`.
//! else if it is zero ref count `Gray` object, it is colored `White` and the cycle is considered to be garbage. The recurisve call of `Scan` continue.
//! 4.3. CollectRoots, at this stage, there is no `Gray` object left, and all `White` object are garbage, we can simply go from root buffer and collect all `White` object for final garbage release,
//! just need to note that when `CollectWhite` those `buffered` object do not need to be freed, since they are already buffered for later release.

mod collector;
mod header;
pub(crate) mod utils;
pub use crate::object::gc::collector::{
    collect, isenabled, pausing, resuming, setenabled, try_collect,
};
use crate::PyObject;
pub(crate) use header::{GcHeader, GcHeaderInner};
pub(crate) use utils::GCReadLock;
type GcObj = PyObject;
type GcObjRef<'a> = &'a GcObj;
