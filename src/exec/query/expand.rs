use dovahkiin::{expr::serde::Expr};
use crate::exec::symbols::{neb_id_symbol_obj};

use crate::{exec::{symbols::objs::*}, ram::schema};

pub trait Macro: Sync {
    fn expand(&self, expr: Expr) -> Result<Expr, String>;
}

pub trait Expand {
    fn expand(self) -> Result<Expr, String>;
}

impl Expand for Expr {
    fn expand(self) -> Result<Expr, String> {
        match &self {
            Expr::List(eles) => {
                let first = eles.first();
                if let Some(Expr::Symbol(sym_id, _)) = first {
                    if let Some(expr_sym) = neb_id_symbol_obj(sym_id) {
                        return expr_sym.macro_expand(self);
                    }
                }
            },
            _ => {}
        }
        return Ok(self)
    }
}

impl Macro for SelectCell {
    // (select-cell SCHEMA <FIELDS> <FILTER>)
    // Check if <FIELDS> is provided. If yes, use `id-cell-sel`
    // Check if <FILTER> is provided. If yes, use `filter-shared-value`
    // Can expands to 
    // (filter-shared-value
    //    (id-cell-sel
    //      (cell-id-query SCHEMA)
    //     FIELDS)
    //   FILTER)
    fn expand(&self, expr: Expr) -> Result<Expr, String> {
        if let Expr::List(eles) = expr {
            let mut params = eles.into_iter();
            let _ = params.next();
            let schema = params.next();
            let fields = params.next();
            let filter = params.next();
            let mut expr_res = if let Some(schema) = schema {
                Expr::List(vec![
                    CellIdQuery::as_expr(), 
                    schema.expand()?
                ])
            } else {
                return Err("Schema is required".to_string());
            };
            if let Some(fields) = fields {
                expr_res = Expr::List(vec![
                    IdCellSel::as_expr(),
                    expr_res,
                    fields.expand()? 
                ]);
            } else {
                expr_res = Expr::List(vec![
                    IdCell::as_expr(),
                    expr_res,
                ]);                
            }
            if let Some(filter) = filter {
                expr_res = Expr::List(vec![
                    FilterSharedValue::as_expr(),
                    expr_res,
                    filter.expand()?
                ]); 
            }
            return Ok(expr_res);
        } else {
            unreachable!()
        }
    }
}

impl Macro for Let {
    fn expand(&self, expr: Expr) -> Result<Expr, String> {
        todo!()
    }
}