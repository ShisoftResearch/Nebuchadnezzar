use dovahkiin::expr::{interpreter::Envorinment, SExpr};

pub fn kv_get<'a>(exprs: Vec<SExpr<'a>>, env: &mut Envorinment<'a>) -> Result<SExpr<'a>, String> {
    unimplemented!()
}

pub fn kv_scan<'a>(exprs: Vec<SExpr<'a>>, env: &mut Envorinment<'a>)  -> Result<SExpr<'a>, String> {
    unimplemented!()
}



