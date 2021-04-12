use serde::{Deserialize, Serialize};
use super::funcs::*;
use lazy_static::*;
use std::collections::HashMap;
use bifrost_hasher::hash_str;
use crate::exec::funcs::reduce::group_by::*;
use crate::exec::funcs::reduce::join::*;
use crate::exec::funcs::agg::*;
use crate::exec::funcs::bool::*;
use crate::exec::funcs::comp::*;
use crate::exec::funcs::data_source::*;
use crate::exec::funcs::scalar::*;
use crate::exec::funcs::terraform::*;
use crate::exec::funcs::vectorization::*;


macro_rules! def_funcs {
    ($($func_name: expr => $func: ident;)*) => {
        lazy_static! {
            pub static ref FUNC_MAP: HashMap<u64, Box<dyn Function>> = {
                let mut func_map = HashMap::new();
                $({
                    let f = $func;
                    let box_f: Box<dyn Function> = Box::new(f);
                    func_map.insert(hash_str($func_name), box_f);
                })*
                func_map
            };
        }
    };
}

def_funcs! {
    "group-by" => GroupBy;
    "join-by" => JoinBy;
    "join" => NaturalJoin;
    "sum" => Sum;
    "max" => Max;
    "min" => Min;
    "all" => All;
    "any" => Any;
    "avg" => Average;
    "and" => And;
    "or" => Or;
    "not" => Not;
    "and-not" => AndNot;
    "xor" => Xor;
    "null?" => IsNull;
    "not-null?" => IsNotNull;
    "null-if" => NullIf;
    "=" => Equal;
    "not=" => NotEqual;
    ">" => Greater;
    ">=" => GreaterEqual;
    "<" => Less;
    "<=" => LessEqual;
    "like" => Like;
    "not-like" => NotLike;
    "repeat" => Repeat;
    "make-source" => MakeSource;
    "+" => Add;
    "-" => Subtract;
    "/" => Divide;
    "*" => Multiply;
    "~" => Negate;
    "limit" => Limit;
    "sort-asc" => SortAsc;
    "sort-desc" => SortDesc;
    "filter" => Filter;
    "to-vec" => ToVec;
    "col" => Col;
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
