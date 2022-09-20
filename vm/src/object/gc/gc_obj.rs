use std::sync::Arc;
use crate::{object::gc::{CcSync, GcHeader}, PyObject};

pub struct PyGcObject {
    gc: Arc<CcSync>,
    header: GcHeader,
    inner: PyObject
}