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
pub mod env;