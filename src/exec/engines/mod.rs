// Execution DAG with various engine backends, the types could be
// - Single thread-local, typically for small, size-fixed dataset
// - Multithreading, for large or non-const node-local dataset
// - Distributed, for table or large/streaming external dataset
// Each backend have their own scheduler and each scheduler can have
// their own DAG been generated from push-down instructions from upper-level

// For thread-local executions, the backend can be
// - Value computation (OwnedValue, SharedValue, OwnedValueRef)
// - Scala computation (primitives or native structs)
// - Vector computation (SIMD, GPU, etc.)

// In the DAG, first nodes must be a data source
// Each data source can have only one backend
// To transform one backend to the other, it can use
// an adaptor to shifting from one backend to the other