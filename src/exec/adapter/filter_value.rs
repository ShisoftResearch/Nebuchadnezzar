use super::Adapter;
use dovahkiin::{
    expr::{interpreter::Interpreter, serde::Expr, SExpr},
    integrated::lisp,
    parser::lisp::ParserExpr,
    types::{OwnedValue, SharedValue},
};

pub struct FilterSharedValue<'a> {
    filter: SExpr<'a>,
    interpreter: Interpreter<'a>,
    iter: Box<dyn Iterator<Item = SharedValue<'a>>>,
}

pub struct FilterSharedValueParams {
    filter: Expr,
}

impl<'a> Adapter<SharedValue<'a>, SharedValue<'a>, FilterSharedValueParams>
    for FilterSharedValue<'a>
{
    fn from(
        input: impl Iterator<Item = SharedValue<'a>> + 'static,
        params: FilterSharedValueParams,
    ) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = SharedValue<'a>>> = Box::new(input);
        let filter = params.filter.to_sexpr();
        let interpreter = lisp::get_interpreter();
        Ok(Self {
            iter,
            filter,
            interpreter,
        })
    }
}

impl<'a> Iterator for FilterSharedValue<'a> {
    type Item = SharedValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(c) = self.iter.next() {
                let filter = self.filter.clone();
                self.interpreter.clear();
                self.interpreter
                    .bind("data", SExpr::shared_value(c.clone()));
                let res = filter.eval(self.interpreter.get_env());
                match res {
                    Ok(expr) => {
                        let expr_res = expr.into_val();
                        let eval = match expr_res {
                            Ok(OwnedValue::Bool(false))
                            | Ok(OwnedValue::Null)
                            | Ok(OwnedValue::NA) => false,
                            Err(s) => {
                                error!("malformed proc result, message {}", s);
                                false
                            }
                            _ => true,
                        };
                        if eval {
                            return Some(c);
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
pub struct FilterOwnedValue<'a> {
    filter: SExpr<'a>,
    interpreter: Interpreter<'a>,
    iter: Box<dyn Iterator<Item = OwnedValue>>,
}

pub struct FilterOwnedValueParams {
    filter: Expr,
}

impl<'a> Adapter<OwnedValue, OwnedValue, FilterOwnedValueParams> for FilterOwnedValue<'a> {
    fn from(
        input: impl Iterator<Item = OwnedValue> + 'static,
        params: FilterOwnedValueParams,
    ) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = OwnedValue>> = Box::new(input);
        let filter = params.filter.to_sexpr();
        let interpreter = lisp::get_interpreter();
        Ok(Self {
            iter,
            filter,
            interpreter,
        })
    }
}

impl<'a> Iterator for FilterOwnedValue<'a> {
    type Item = OwnedValue;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(c) = self.iter.next() {
                let filter = self.filter.clone();
                self.interpreter.clear();
                self.interpreter.bind("data", SExpr::owned_value(c));
                let res = filter.eval(self.interpreter.get_env());
                match res {
                    Ok(expr) => {
                        let expr_res = expr.into_val();
                        let eval = match expr_res {
                            Ok(OwnedValue::Bool(false))
                            | Ok(OwnedValue::Null)
                            | Ok(OwnedValue::NA) => false,
                            Err(s) => {
                                error!("malformed proc result, message {}", s);
                                false
                            }
                            _ => true,
                        };
                        if eval {
                            return self.interpreter.unbind("data").unwrap().owned_val();
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
