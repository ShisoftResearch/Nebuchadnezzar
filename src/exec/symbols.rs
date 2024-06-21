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
            fn io_types(&self) -> (DataType, DataType) {
                (DataType::NA, DataType::NA)
            }
        }
    };
}

macro_rules! broadcasting_impl {
    ($symbol:ident) => {
        impl SymbolObj for $symbol {
            fn symbol_type(&self) -> SymbolType {
                SymbolType::Broadcasting
            }
            fn io_types(&self) -> (DataType, DataType) {
                unimplemented!()
            }
        }
    };
}

macro_rules! partitioning_impl {
    ($symbol:ident, $tinput:expr, $toutput:expr) => {
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
            fn io_types(&self) -> (DataType, DataType) {
                ($tinput, $toutput)
            }
        }
    };
}

macro_rules! compute_impl {
    ($symbol:ident, $tinput:expr, $toutput:expr) => {
        impl SymbolObj for $symbol {
            fn io_types(&self) -> (DataType, DataType) {
                ($tinput, $toutput)
            }
        }
    };
}

macro_rules! aggregation_impl {
    ($symbol:ident, $tinput:expr, $toutput:expr) => {
        impl SymbolObj for $symbol {
            fn symbol_type(&self) -> SymbolType {
                SymbolType::Aggregation
            }
            fn io_types(&self) -> (DataType, DataType) {
                ($tinput, $toutput)
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

    ($sym_name:expr => $symbol:ident ($tinput:expr => $toutput:expr) - [P], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [partitioning_impl!($symbol, $tinput, $toutput);],
        }
    };

    ($sym_name:expr => $symbol:ident - [B], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [broadcasting_impl!($symbol);],
        }
    };

    ($sym_name:expr => $symbol:ident ($tinput:expr => $toutput:expr) - [A], $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [aggregation_impl!($symbol, $tinput, $toutput);],
        }
    };

    ($sym_name:expr => $symbol:ident ($tinput:expr => $toutput:expr), $($rest:tt)*) => {
        def_symbols! {
            $($rest)*
            $sym_name => $symbol - [compute_impl!($symbol, $tinput, $toutput);],
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
                NEB_SYMBOL_OBJS.get(&(*self as u64)).unwrap().symbol_type()
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
    Compute, // Default, compute on bulk of data
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
    fn symbol_type(&self) -> SymbolType {
        SymbolType::Compute
    }
    fn compute(&self, data: *mut ()) -> *mut () {
        return ptr::null_mut();
    }
    fn io_types(&self) -> (DataType, DataType);
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
    Type,
}

pub enum BasicType {
    Expr,
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
}

def_symbols! {
    // Comparators
    "=" => Equal (Either(Dynamic) => Either(Bool)),
    "!=" => NotEqual (Either(Dynamic) => Either(Bool)),
    ">" => Greater (Either(Dynamic) => Either(Bool)),
    ">=" => GreaterEqual (Either(Dynamic) => Either(Bool)),
    "<" => Less  (Either(Dynamic) => Either(Bool)),
    "<=" => LessEqual (Either(Dynamic) => Either(Bool)),
    "like" => Like (Either(Dynamic) => Either(Bool)),
    "not-like" => NotLike (Either(Dynamic) => Either(Bool)),

    // Boolean
    "and" => And (Either(Bool) => Either(Bool)),
    "and-not" => AndNot (Either(Bool) => Either(Bool)),
    "not" => Not (Either(Bool) => Either(Bool)),
    "or" => Or (Either(Bool) => Either(Bool)),
    "xor" => Xor (Either(Bool) => Either(Bool)),
    "not-null?" => NotNull (Either(Dynamic) => Either(Bool)),
    "null?" => IsNull (Either(Dynamic) => Either(Bool)),

    // Containment Tests
    "regex-matches" => RegexMatches (Either(Str) => Either(Bool)),
    "is-in?" => IsIn (Either(Expr) => Either(Bool)),

    "cast" => Cast (Either(Dynamic) => Either(Anything)),
    "can-cast?" => CanCast (Either(Dynamic) => Either(Bool)),

    //***** NARROW *****//
    "filter-map" => FilterMap (Stream(Dynamic) => Stream(Dynamic)),
    "filter" => Filter (Stream(Dynamic) => Stream(Dynamic)),
    "map" => Map (Stream(Dynamic) => Stream(Anything)),

    // Final Iterater
    "concat" => Concat (Stream(Dynamic) => Stream(Dynamic)),
    "limit" => Limit (Stream(Dynamic) => Stream(Dynamic)),

    //***** WIDE *****//
    // "sort-by" => SortBy (Stream(SharedValue) => Stream(SharedValue)) - [P],
    // "sort-by-asc" => SortByASC (Stream(SharedValue) => Stream(SharedValue)) - [P],
    // "sort-by-desc" => SortByDESC (Stream(SharedValue) => Stream(SharedValue)) - [P],
    // "group-by" => GroupBy (Stream(SharedValue) => Stream(SharedValue)) - [P],
    // "join" => Join (Stream(SharedValue) => Stream(SharedValue)) - [P],
    // "full-join" => FullJoin (Stream(SharedValue) => Stream(SharedValue)) - [P],
    // "natural-join" => NaturalJoin (Stream(SharedValue) => Stream(SharedValue)) - [P],
    // "reduce" => Reduce (Stream(Dynamic) => Stream(Any)) - [P],

    // Aggregations
    "all" => All (Stream(Dynamic) => Scala(Bool)) - [A],
    "any" => Any (Stream(Dynamic) => Scala(Bool)) - [A],
    "count" => Count (Stream(Dynamic) => Scala(U64)) - [A],
    "avg" => Average (Stream(Num) => Scala(F64)) - [A],
    "max" => Max (Stream(Num) => Scala(Num)) - [A],
    "min" => Min (Stream(Num) => Scala(Num)) - [A],
    "sum" => Sum (Stream(Num) => Scala(Num)) - [A],
    "find" => Find (Stream(Dynamic) => Scala(Dynamic)) - [A],

    //*** Data source ***/
    "cell-id-query" => CellIdQuery (Nothing => Stream(Id)),
    "repeat" => Repeat (Scala(Anything) => Stream(Dynamic)),

    //*** Adapter  ***/
    "id-cell" => IdCell (Stream(Id) => Stream(SharedCell)) - [P],
    "id-cell-sel" => IdCellSel (Stream(Id) => Stream(SharedCell)) - [P],
    "borrow-cell-value" => BorrowCellValue (Stream(SharedCell) => Stream(SharedValue)),
    "owned-cell-value" => OwnedCellValue (Stream(SharedCell) => Stream(OwnedValue)),
    "filter-shared-value" => FilterSharedValue (Stream(SharedValue) => Stream(SharedValue)),
    "filter-owned-value" => FilterOwnedValue (Stream(OwnedValue) => Stream(OwnedValue)),
    "to-owned-cell" => ToOwnedCell (Stream(SharedCell) => Stream(OwnedCell)),
    "proc-shared-value" => ProcSharedValue (Stream(SharedValue) => Stream(SharedValue)),
    "proc-owned-value" => ProcOwnedValue (Stream(OwnedValue) => Stream(OwnedValue)),
    "take" => Take (Stream(Anything) => Stream(Dynamic)),

    // //*** Partitioner ***/
    // "hash-partition" => HashPartition,
    // "range-partition" => RangePartition,

    //*** Bindings ***/
    "let" => Let - [M],
    "bind" => Bind (Nothing => Either(Anything)), // This is the final form

    //*** Macro ***/
    "select-cell" => SelectCell - [M],

    // Preprocess of parameters not in the NebSymbol list
    "loc-do" => LocalDo (Either(Dynamic) => Either(Anything)),
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
