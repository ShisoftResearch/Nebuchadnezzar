use dovahkiin::expr::symbols::SysSymbol as DovSymbol;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use dovahkiin::ahash::HashMap;
use dovahkiin::ahash::HashMapExt;

macro_rules! def_symbols {
    ($($sym_name: expr => $symbol: ident,)*) => {
        lazy_static! {
            pub static ref NEB_SYMBOL_MAP: HashMap<u64, NebSymbol> = {
                let mut sym_map = HashMap::new();
                $({
                    sym_map.insert(hash_ident!($sym_name), NebSymbol::$symbol);
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

    };
}

def_symbols! {
    // Comparators
    "=" => Equal,
    "!=" => NotEqual,
    ">" => Greater,
    ">=" => GreaterEqual,
    "<" => Less,
    "<=" => LessEqual,
    "like" => Like,
    "not-like" => NotLike,

    // Boolean
    "and" => And,
    "and-not" => AndNot,
    "not" => Not,
    "or" => Or,
    "xor" => Xor,
    "not-null?" => NotNull,
    "null?" => IsNull,

    // Containment Tests
    "regex-matches" => RegexMatches,
    "is-in?" => IsIn,

    "cast" => Cast,
    "can-cast?" => CanCast,

    //***** NARROW *****//
    "filter-map" => FilterMap,
    "filter" => Filter,
    "map" => Map,

    // Final Iterater
    "concat" => Concat,
    "limit" => Limit,

    //***** WIDE *****//
    "sort-by" => SortBy,
    "sort-by-asc" => SortByASC,
    "sort-by-desc" => SortByDESC,
    "group-by" => GroupBy,
    "join" => Join,
    "full-join" => FullJoin,
    "natural-join" => NaturalJoin,
    "reduce" => Reduce,

    // Aggregations
    "all" => All,
    "any" => Any,
    "count" => Count,
    "avg" => Average,
    "max" => Max,
    "min" => Min,
    "sum" => Sum,
    "find" => Find,
    
    //*** Data source ***/
    "cell-id-query" => CellIdQuery,
    "repeat" => Repeat,

    //*** Adapter  ***/
    "id-cell" => IdCell,
    "id-cell-sel" => IdCellSel,
    "borrow-cell-value" => BorrowCellValue,
    "owned-cell-value" => OwnedCellValue,
    "filter-shared-value" => FilterSharedValue,
    "filter-owned-value" => FilterOwnedValue,
    "to-owned-cell" => ToOwnedCell,
    "proc-shared-value" => ProcSharedValue,
    "proc-owned-value" => ProcOwnedValue,
    "ref-cell" => RefCell,
    "take" => Take,

    //*** Partitioner ***/
    "hash-partition" => HashPartition,
    "range-partition" => RangePartition,
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