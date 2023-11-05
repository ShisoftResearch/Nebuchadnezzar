use ::serde::{Deserialize, Serialize};
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use dovahkiin::expr::interpreter::Envorinment;
use dovahkiin::expr::symbols::*;
use dovahkiin::expr::SExpr;
use lazy_static::*;
use std::collections::HashMap;

mod datasource;

macro_rules! defdbsymbols {
    ($($sym: expr => $name: ident, $is_macro: expr, $eval: expr);*) => {
        $(
            #[derive(Debug)]
            pub struct $name;
            impl dovahkiin::expr::symbols::Symbol for $name {
                fn eval<'a>(&self, exprs: Vec<SExpr<'a>>, env: &mut Envorinment<'a>) -> Result<SExpr<'a>, String> where Self: Sized {
                    $eval(exprs, env)
                }
                fn is_macro(&self) -> bool {
                    return $is_macro;
                }
            }
        )*
        pub fn init_symbols() {
            let mut map = ISYMBOL_MAP.map.borrow_mut();
            $(
                assert!(!map.contains_key(&hash_ident!($sym)), "symbol {} already exists", $sym);
                map.insert(hash_ident!($sym), Box::new($name));
            )*
        }

        #[derive(Copy, Clone)]
        pub enum DBSymbols {
            Sys(SysSymbols),
            $(
                $name,
            )*
        }
        impl DBSymbols {
            pub fn from_id(id: u64) -> Self {
                match id {
                    $(
                        hash_ident!($sym) => Self::$name,
                    )*
                    o => DBSymbols::Sys(SysSymbols::from_id(o))
                }
            }
            pub fn to_id(self) -> u64 {
                match self {
                    Self::Sys(sys) => sys as u64,
                    $(
                        Self::$name => hash_ident!($sym),
                    )*
                }
            }
        }
    };
}

defdbsymbols! {
    "kv-get" => KVGet, false, |exprs, env| {
        datasource::kv_get(exprs, env)
    };
    "kv-scan" => KVScan, true, |exprs, env| {
        datasource::kv_scan(exprs, env)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Symbol {
    // Aggregations
    All,
    Any,
    Count,
    Average,
    Max,
    Min,
    Sum,

    // Scalars
    Add,
    Subtract,
    Divide,
    Multiply,
    Negate,

    // Comparators
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Like,
    NotLike,

    // Boolean
    And,
    AndNot,
    Not,
    Or,
    Xor,
    NotNull,
    IsNull,
    NullIf,

    // Containment Tests
    StringMatches,
    IndexIn,
    IsIn,

    // String Predicates
    StringIsDecimal,
    StringIsDigit,
    StringIsLowerCase,
    StringIsUpperCase,
    StringIsNumeric,
    StringIsSpace,
    SubString,

    // String Transforms
    StringToUpper,
    StringToLower,
    StringLength,

    Cast,
    CanCast,

    Concat,
    Filter,
    Limit,
    Take,

    SortByASC,
    SortByDESC,
}
