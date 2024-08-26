use std::ptr;

use bifrost_plugins::hash_ident;
use dovahkiin::ahash::HashMap;
use dovahkiin::ahash::HashMapExt;
use dovahkiin::expr::serde::Expr;
use dovahkiin::expr::symbols::SysSymbol as DovSymbol;

use super::partitioner::Partitioner;
use super::query::env::Environment;
use super::query::expand::Macro;
use crate::exec::query::partitioning::Partitioning;

macro_rules! macro_impl {
    ($symbol:ident) => {
        impl SymbolObj for $symbol {
            fn symbol_type(&self) -> SymbolType {
                SymbolType::Macro
            }
            #[inline(always)]
            fn macro_expand(&self, expr: Expr, env: &mut Environment) -> Result<Expr, String> {
                self.expand(expr, env)
            }
            fn io_types(&self) -> (Vec<DataType>, DataType) {
                (vec![], DataType::NA)
            }
        }
    };
}

macro_rules! partitioning_impl {
    ($symbol:ident, [$($tinput:expr),*], $toutput:expr) => {
        impl SymbolObj for $symbol {
            fn symbol_type(&self) -> SymbolType {
                SymbolType::Partitioning
            }
            fn partition_init(
                &self,
                expr: &Expr,
                env: &mut Environment,
            ) -> Result<Option<Box<dyn Partitioner>>, String> {
                self.get_partitioner(expr, env)
            }
            fn partition_data(
                &self,
                data_ptr: *mut (),
                env: &mut Environment,
                partitioner: &Box<dyn Partitioner>,
            ) -> Option<u64> {
                self.get_partition(data_ptr, env, partitioner)
            }
            fn io_types(&self) -> (Vec<DataType>, DataType) {
                (vec![$($tinput),*], $toutput)
            }
        }
    };
}

macro_rules! transformer_impl {
    ($symbol:ident, [$($tinput:expr),*], $toutput:expr) => {
        impl SymbolObj for $symbol {
            fn symbol_type(&self) -> SymbolType {
                SymbolType::Transformer
            }
            fn io_types(&self) -> (Vec<DataType>, DataType) {
                (vec![$($tinput),*], $toutput)
            }
        }
    };
}

macro_rules! operation_impl {
    ($symbol:ident, [$($tinput:expr),*], $toutput:expr) => {
        impl SymbolObj for $symbol {
            fn symbol_type(&self) -> SymbolType {
                SymbolType::Operation
            }
            fn io_types(&self) -> (Vec<DataType>, DataType) {
                (vec![$($tinput),*], $toutput)
            }
        }
    };
}

macro_rules! aggregation_impl {
    ($symbol:ident, [$($tinput:expr),*], $toutput:expr) => {
        impl SymbolObj for $symbol {
            fn symbol_type(&self) -> SymbolType {
                SymbolType::Aggregation
            }
            fn io_types(&self) -> (Vec<DataType>, DataType) {
                (vec![$($tinput),*], $toutput)
            }
        }
    };
}

macro_rules! def_symbols {
    ($sym_name:expr => $symbol:ident - [M], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [macro_impl!($symbol);],
        }
    };

    ($sym_name:expr => $symbol:ident ([$($tinput:expr),*] => $toutput:expr) - [P], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [partitioning_impl!($symbol, [$($tinput),*], $toutput);],
        }
    };

    ($sym_name:expr => $symbol:ident ([$($tinput:expr),*] => $toutput:expr) - [A], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [aggregation_impl!($symbol, [$($tinput),*], $toutput);],
        }
    };

    ($sym_name:expr => $symbol:ident ([$($tinput:expr),*] => $toutput:expr) - [O], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [operation_impl!($symbol, [$($tinput),*], $toutput);],
        }
    };

    ($sym_name:expr => $symbol:ident ([$($tinput:expr),*] => $toutput:expr) - [T], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [transformer_impl!($symbol, [$($tinput),*], $toutput);],
        }
    };

    ($($sym_name: expr => $symbol: ident - [$tr:item],)*) => {
        lazy_static! {
            pub static ref NEB_SYMBOL_MAP: HashMap<u64, NebSymbol> = {
                let mut sym_map = HashMap::new();
                $({
                    sym_map.insert(hash_ident!($sym_name), NebSymbol::$symbol);
                })*
                sym_map
            };
            pub static ref NEB_SYMBOL_OBJS: HashMap<u64, Box<dyn SymbolObj>> = {
                let mut sym_map = HashMap::new();
                $({
                    let boxed: Box<dyn SymbolObj> = Box::new(objs::$symbol);
                    sym_map.insert(hash_ident!($sym_name), boxed);
                })*
                sym_map
            };
        }

        pub fn neb_symbol_id(symbol: NebSymbol) -> u64 {
            symbol as u64
        }

        pub fn neb_id_symbol (symbol_id: u64) -> Option<NebSymbol> {
            NEB_SYMBOL_MAP.get(&symbol_id).cloned()
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum NebSymbol {
            $(
                $symbol = hash_ident!($sym_name),
            )*
        }
        impl NebSymbol {
            pub fn symbol_type(&self) -> SymbolType {
                self.symbol_obj().symbol_type()
            }
            pub fn symbol_obj(&self) -> &Box<dyn SymbolObj> {
                &NEB_SYMBOL_OBJS[&(*self as u64)]
            }
        }
        pub mod objs {
            use dovahkiin::expr::serde::Expr;
            use super::*;
            $(
                pub struct $symbol;
                $tr
                impl $symbol {
                    pub fn as_expr() -> Expr {
                        Expr::Symbol(NebSymbol::$symbol as u64, $sym_name.to_string())
                    }
                }
            )*
        }

        pub fn neb_id_symbol_obj<'a>(symbol_id: &u64) -> Option<&'a Box<dyn SymbolObj>> {
            NEB_SYMBOL_OBJS.get(symbol_id)
        }
    };
}

#[derive(PartialEq, Eq)]
pub enum SymbolType {
    Macro,
    Partitioning, // Partition, then compute
    Broadcasting,
    Aggregation,
    Transformer, // Default, compute on bulk of data
    Operation,   // Operation embeded in computation
}

pub trait SymbolObj: Sync {
    fn macro_expand(&self, expr: Expr, _env: &mut Environment) -> Result<Expr, String> {
        Ok(expr)
    }
    fn partition_init(
        &self,
        _expr: &Expr,
        _env: &mut Environment,
    ) -> Result<Option<Box<dyn Partitioner>>, String> {
        Ok(None)
    }
    fn partition_data(
        &self,
        _data_ptr: *mut (),
        _env: &mut Environment,
        _partitioner: &Box<dyn Partitioner>,
    ) -> Option<u64> {
        unreachable!()
    }
    fn symbol_type(&self) -> SymbolType;
    fn compute(&self, _data: *mut ()) -> *mut () {
        return ptr::null_mut();
    }
    fn io_types(&self) -> (Vec<DataType>, DataType);
}

use BasicType::*;
use DataType::*;
pub enum DataType {
    // For type checking only
    NA,
    Nothing,
    Stream(BasicType),
    Scala(BasicType),
    Either(BasicType),
    Array(BasicType),
    Type,
    Expr,
}

pub enum BasicType {
    OwnedValue,
    OwnedValueRef,
    SharedValue,
    OwnedCell,
    OwnedCellRef,
    SharedCell,
    Id,
    Num,
    U64,
    F64,
    Bool,
    Str,
    Dynamic,
    Anything,
    Array
}

def_symbols! {
    // Comparators
    "=" => Equal ([Either(Dynamic), Either(Dynamic)] => Either(Bool)) - [O],
    "!=" => NotEqual ([Either(Dynamic), Either(Dynamic)] => Either(Bool)) - [O],
    ">" => Greater ([Either(Dynamic), Either(Dynamic)] => Either(Bool)) - [O],
    ">=" => GreaterEqual ([Either(Dynamic), Either(Dynamic)] => Either(Bool)) - [O],
    "<" => Less  ([Either(Dynamic), Either(Dynamic)] => Either(Bool)) - [O],
    "<=" => LessEqual ([Either(Dynamic), Either(Dynamic)] => Either(Bool)) - [O],
    "like" => Like ([Either(Dynamic)] => Either(Bool)) - [O],
    "not-like" => NotLike ([Either(Dynamic)] => Either(Bool)) - [O],

    // Boolean
    "and" => And ([Either(Bool), Either(Bool)] => Either(Bool)) - [O],
    "and-not" => AndNot ([Either(Bool), Either(Bool)] => Either(Bool)) - [O],
    "not" => Not ([Either(Bool)] => Either(Bool)) - [O],
    "or" => Or ([Either(Bool), Either(Bool)] => Either(Bool)) - [O],
    "xor" => Xor ([Either(Bool), Either(Bool)] => Either(Bool)) - [O],
    "not-null?" => NotNull ([Either(Dynamic)] => Either(Bool)) - [O],
    "null?" => IsNull ([Either(Dynamic)] => Either(Bool)) - [O],

    // Containment Tests
    "regex-matches" => RegexMatches ([Scala(Str), Either(Str)] => Either(Bool)) - [O],
    "is-in?" => IsIn ([Either(Bool), Either(BasicType::Array)] => Either(Bool)) - [O],

    "cast" => Cast ([Type, Either(Dynamic)] => Either(Anything)) - [O],
    "can-cast?" => CanCast ([Type, Either(Dynamic)] => Either(Bool)) - [O],

    //***** NARROW *****//
    "filter-map" => FilterMap ([Stream(Dynamic)] => Stream(Dynamic)) - [T],
    "filter" => Filter ([Stream(Dynamic)] => Stream(Dynamic)) - [T],
    "map" => Map ([Stream(Dynamic)] => Stream(Anything)) - [T],

    // Final Iterater
    "concat" => Concat ([Stream(Dynamic), Stream(Dynamic)] => Stream(Dynamic)) - [T],
    "limit" => Limit ([Scala(Num), Stream(Dynamic)] => Stream(Dynamic)) - [T],

    //***** WIDE *****//
    // "sort" => SortBy (Stream(SharedValue) => Stream(SharedValue)) - [P],
    "sort-by-asc" => SortByASC ([Stream(Num), Stream(SharedValue)] => Stream(SharedValue)) - [P],
    "sort-by-desc" => SortByDESC ([Stream(Num), Stream(SharedValue)] => Stream(SharedValue)) - [P],
    "join" => Join ([Expr, Stream(SharedValue), Stream(SharedValue)] => Stream(SharedValue)) - [P],
    "full-join" => FullJoin ([Expr, Stream(SharedValue)] => Stream(SharedValue)) - [P],
    "group-by" => GroupBy ([Expr, Stream(SharedValue)] => Stream(SharedValue)) - [P],
    "reduce" => Reduce ([Expr, Stream(Dynamic)] => Stream(Anything)) - [P],

    // Aggregations
    "all" => All ([Stream(Bool), Stream(Dynamic)] => Scala(Bool)) - [A],
    "any" => Any ([Stream(Bool), Stream(Dynamic)] => Scala(Bool)) - [A],
    "count" => Count ([Stream(Dynamic)] => Scala(U64)) - [A],
    "avg" => Average ([Stream(Num)] => Scala(F64)) - [A],
    "max" => Max ([Stream(Num)] => Scala(Num)) - [A],
    "min" => Min ([Stream(Num)] => Scala(Num)) - [A],
    "sum" => Sum ([Stream(Num)] => Scala(Num)) - [A],
    "find" => Find ([Stream(Bool), Stream(Dynamic)] => Scala(Dynamic)) - [A],

    //*** Data source ***/
    "cell-id-query" => CellIdQuery ([Expr] => Stream(Id)) - [T],
    "repeat" => Repeat ([Scala(Num), Scala(Anything)] => Stream(Dynamic)) - [T],

    //*** Adapter  ***/
    "id-cell" => IdCell ([Stream(Id)] => Stream(SharedCell)) - [P],
    "id-cell-sel" => IdCellSel ([Expr, Stream(Id)] => Stream(SharedCell)) - [P],
    "borrow-cell-value" => BorrowCellValue ([Stream(SharedCell)] => Stream(SharedValue)) - [T],
    "owned-cell-value" => OwnedCellValue ([Stream(SharedCell)] => Stream(OwnedValue)) - [T],
    "filter-shared-value" => FilterSharedValue ([Expr, Stream(SharedValue)] => Stream(SharedValue)) - [T],
    "filter-owned-value" => FilterOwnedValue ([Expr, Stream(OwnedValue)] => Stream(OwnedValue)) - [T],
    "to-owned-cell" => ToOwnedCell ([Stream(SharedCell)] => Stream(OwnedCell)) - [T],
    "proc-shared-value" => ProcSharedValue ([Expr, Stream(SharedValue)] => Stream(SharedValue)) - [T],
    "proc-owned-value" => ProcOwnedValue ([Expr, Stream(OwnedValue)] => Stream(OwnedValue)) - [T],
    "take" => Take ([Scala(Num), Stream(Anything)] => Stream(Dynamic)) - [T],

    // //*** Partitioner ***/
    // "hash-partition" => HashPartition,
    // "range-partition" => RangePartition,

    //*** Bindings ***/
    "let" => Let - [M],
    "bind" => Bind ([Expr] => Either(Anything)) - [O], // This is the final form

    //*** Macro ***/
    "select-cell" => SelectCell - [M],

    // Preprocess of parameters not in the NebSymbol list
    "loc-do" => LocalDo ([Expr, Either(Dynamic)] => Either(Anything)) - [O],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Symbol {
    Neb(NebSymbol),
    Dov(DovSymbol),
}

pub fn symbol_from_id(id: u64) -> Option<Symbol> {
    if let Some(neb_sym) = neb_id_symbol(id) {
        Some(Symbol::Neb(neb_sym))
    } else if let Some(dov_sym) = DovSymbol::from_id(id) {
        Some(Symbol::Dov(dov_sym))
    } else {
        None
    }
}
