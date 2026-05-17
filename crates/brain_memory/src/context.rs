//! Shared semantic context for memory operations.
//!
//! Composes the SQLite-backed stores with the optional LanceDB +
//! embedder search layer. When the search layer is absent, write
//! operations still work but read/retrieval ops return an error
//! asking the user to download the embedding model.
