// The intepreter should try pickup with 'NebSymbol's in the expression
// and then falls back to 'DovSymbol's.
// When decoding with 'NebSymbol's, the expression would be expanded
// With the coorsponding symbol behavior. If would decide if it should
// continue with 'NebSymbol's, 'DovSymbol's or reject.

// Execute order: Expand, Plan

pub mod env;
pub mod expand;
pub mod planner;
