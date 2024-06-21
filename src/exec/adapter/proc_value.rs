use super::Adapter;
use dovahkiin::{
    expr::{interpreter::Interpreter, serde::Expr, SExpr},
    integrated::lisp,
    parser::lisp::ParserExpr,
    types::{OwnedValue, SharedValue},
};

pub struct ProcSharedValue<'a> {
    proc: SExpr<'a>,
    interpreter: Interpreter<'a>,
    iter: Box<dyn Iterator<Item = SharedValue<'a>>>,
}

pub struct ProcSharedValueParams {
    proc: Expr,
}

impl<'a> Adapter<SharedValue<'a>, OwnedValue, ProcSharedValueParams> for ProcSharedValue<'a> {
    fn from(
        input: impl Iterator<Item = SharedValue<'a>> + 'static,
        params: ProcSharedValueParams,
    ) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = SharedValue<'a>>> = Box::new(input);
        let proc = params.proc.to_sexpr();
        let interpreter = lisp::get_interpreter();
        Ok(Self {
            iter,
            proc,
            interpreter,
        })
    }
}

impl<'a> Iterator for ProcSharedValue<'a> {
    type Item = OwnedValue;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(c) = self.iter.next() {
                let processor = self.proc.clone();
                self.interpreter.clear();
                unsafe {
                    self.interpreter.unsafe_set_global_val(&c);
                }
                let res = processor.eval(self.interpreter.get_env());
                self.interpreter.unset_global_val();
                match res {
                    Ok(expr) => {
                        let expr_res = expr.into_val();
                        match expr_res {
                            Ok(data) => return Some(data),
                            Err(s) => {
                                error!("malformed proc result, message {}", s);
                            }
                        }
                    }
                    Err(s) => {
                        error!("Error on processing, message {}", s);
                    }
                }
            } else {
                return None;
            }
        }
    }
}

// For Owned value
pub struct ProcOwnedValue<'a> {
    proc: SExpr<'a>,
    interpreter: Interpreter<'a>,
    iter: Box<dyn Iterator<Item = OwnedValue>>,
}

pub struct ProcOwnedValueParams {
    proc: Expr,
}

impl<'a> Adapter<OwnedValue, OwnedValue, ProcOwnedValueParams> for ProcOwnedValue<'a> {
    fn from(
        input: impl Iterator<Item = OwnedValue> + 'static,
        params: ProcOwnedValueParams,
    ) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = OwnedValue>> = Box::new(input);
        let proc = params.proc.to_sexpr();
        let interpreter = lisp::get_interpreter();
        Ok(Self {
            iter,
            proc,
            interpreter,
        })
    }
}

impl<'a> Iterator for ProcOwnedValue<'a> {
    type Item = OwnedValue;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(c) = self.iter.next() {
                let processor = self.proc.clone();
                self.interpreter.clear();
                unsafe {
                    self.interpreter.unsafe_set_global_val(&c.shared());
                };
                let res = processor.eval(self.interpreter.get_env());
                self.interpreter.unset_global_val();
                match res {
                    Ok(expr) => {
                        let expr_res = expr.into_val();
                        match expr_res {
                            Ok(data) => return Some(data),
                            Err(s) => {
                                error!("malformed proc result, message {}", s);
                            }
                        }
                    }
                    Err(s) => {
                        error!("Error on processing, message {}", s);
                    }
                }
            } else {
                return None;
            }
        }
    }
}
