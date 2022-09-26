use std::{collections::HashMap, ptr::NonNull, sync::Arc};

use super::{header::Color, GcObjPtr, CcSync};

type ObjPtr = NonNull<dyn GcObjPtr>;
struct VizObj {
    ptr: ObjPtr,
    ref_cnt: usize,
    color: Color,
    buffered: bool,
}

type Edges = (ObjPtr, ObjPtr);

struct ObjectGraph {
    cc: Arc<CcSync>,
    obj_sets: HashMap<ObjPtr, VizObj>,
    edges: Vec<Edges>,
}

impl<'a> dot::Labeller<'a, VizObj, Edges> for ObjectGraph {
    fn graph_id(&'a self) -> dot::Id<'a> {
        dot::Id::new("Object Graph").unwrap()
    }

    fn node_id(&'a self, n: &VizObj) -> dot::Id<'a> {
        dot::Id::new(format!("{:?}", n.ptr)).unwrap()
    }
    fn node_color(&'a self, node: &VizObj) -> Option<dot::LabelText<'a>> {
        let color = match &node.color {
            Color::Black => "black",
            Color::Gray => "gray",
            Color::White => "white",
            Color::Purple => "purple",
        };
        Some(dot::LabelText::label(color))
    }
}
