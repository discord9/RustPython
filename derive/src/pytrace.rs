use proc_macro2::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput, Result};

pub(crate) fn impl_pytrace(attr: AttributeArgs, mut item: DeriveInput) -> Result<TokenStream> {
    if !attr.is_empty() {
        panic!(
            "pytrace macro expect no attr(s), found {} attr(s)",
            attr.len()
        );
    }
    let ty = &item.ident;

    let trace_code = match &mut item.data {
        syn::Data::Struct(s) => {
            let fields = &mut s.fields;
            if let syn::Fields::Named(ref mut fields) = fields {
                let res: Vec<_> = fields
                    .named
                    .iter_mut()
                    .map(|f| {
                        let name = f
                            .ident
                            .as_ref()
                            .expect("Field should have a name in non-tuple struct");
                        let mut do_trace = true;
                        f.attrs.retain(|attr| {
                            // remove #[notrace] and not trace this specifed field
                            if attr.path.segments.last().unwrap().ident == "notrace" {
                                do_trace = false;
                                false
                            } else {
                                true
                            }
                        });
                        if do_trace {
                            quote!(
                                self.#name.trace(tracer_fn);
                            )
                        } else {
                            quote!()
                        }
                    })
                    .collect();
                res
            } else {
                panic!("Expect only Named fields")
            }
        }
        syn::Data::Enum(_) => todo!(),
        syn::Data::Union(_) => todo!(),
    };

    let trace_code = trace_code
        .into_iter()
        .fold(quote! {}, |acc, new| quote! { #acc #new });

    let ret = quote! {
        #item
        #[cfg(feature = "gc_bacon")]
        unsafe impl ::rustpython_vm::object::Trace for #ty {
            fn trace(&self, tracer_fn: &mut ::rustpython_vm::object::TracerFn) {
                #trace_code
            }
        }
    };
    Ok(ret)
}
