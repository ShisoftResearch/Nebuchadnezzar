// Example query:
// 1.
// (select-cell SCHEMA <FIELDS> <FILTER>)
// Check if <FIELDS> is provided. If yes, use `id-cell-sel`
// Check if <FILTER> is provided. If yes, use `filter-shared-value`
// Can expands to 
// (filter-shared-value
//    (id-cell-sel
//      (cell-id-query SCHEMA)
//     FIELDS)
//   FILTER)
// 
// Delaing with 'let' binding expression
// The intepreter should consider 'let' as a node in the DAG
// It also need to trace the dataflow of the binging
// to construct the DAG

// The intepreter should try pickup with 'NebSymbol's in the expression
// and then falls back to 'DovSymbol's. 
// When decoding with 'NebSymbol's, the expression would be expanded
// With the coorsponding symbol behavior. If would decide if it should
// continue with 'NebSymbol's, 'DovSymbol's or reject.

pub mod expand;