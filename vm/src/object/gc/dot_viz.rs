use std::ptr::NonNull;

use super::{GcObjPtr, header::Color};

struct VizObj{
    ptr: NonNull<dyn GcObjPtr>,
    ref_cnt: usize,
    color: Color,
    buffered: bool
}