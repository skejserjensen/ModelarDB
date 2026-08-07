# ModelarDB Development
This document describes the structure of the code and general considerations to consider when doing further development.
As such, this document should be used as a guideline when contributing to the repository. Contributions to all aspects
of ModelarDB are highly appreciated and do not need to be in the form of new features or even code. For example,
contributions can be:

- Helping other users.
- Writing documentation.
- Testing features and reporting bugs.
- Writing unit tests and integration tests.
- Fixing bugs in existing functionality.
- Refactoring existing functionality.
- Implementing new functionality.

Any questions or discussions regarding a possible contribution should be posted in the appropriate GitHub issue if one
exists, e.g., the bug report if it is a bugfix, and as a new GitHub issue otherwise.

## Structure
The ModelarDB project consists of the following crates and major components:

- [modelardb_bulkloader](/crates/modelardb_bulkloader) - ModelarDB's command-line bulk loader in the form of the binary
`modelardbb`.
- [modelardb_client](/crates/modelardb_client) - ModelarDB's command-line client in the form of the binary `modelardb`.
  - **Error** - Error type used throughout the crate, a single error type is used for simplicity.
  - **Helper** - Enhances the command-line client with autocompletion of keywords and names.
- [modelardb_compression](/crates/modelardb_compression) - Library providing lossless and lossy model-based compression
of time series.
  - **Models** - Multiple types of models used for compressing time series within different kinds of error bounds
  (possibly 0% error).
  - **Compression** - Compresses univariate time series within user-defined error bounds (possibly 0% error) and outputs
  compressed segments.
  - **Error** - Error type used throughout the crate, a single error type is used for simplicity.
  - **Types** - Types used throughout the crate, e.g., for creating compressed segments and accumulating batches of
  them.
- [modelardb_embedded](/crates/modelardb_embedded) - Library for reading from and writing to ModelarDB instances and
data folders from programming languages.
  - **Error** - Error type used throughout the crate, a single error type is used for simplicity.
  - **C-API** - A C-API for using modelardb_embedded from other programming languages through a C-FFI.
  - **ModelarDB** - Module providing functionality for reading from and writing to ModelarDB instances and data folders.
- [modelardb_manager](/crates/modelardb_manager) - ModelarDB's manager in the form of the binary `modelardbm`.
  - **Cluster** - Manages edge and cloud nodes currently controlled by the ModelarDB manager and provides functionality
  for balancing query workloads across multiple cloud nodes.
  - **Error** - Error type used throughout the crate, a single error type is used for simplicity.
  - **Metadata** - Manages metadata stored in Delta Lake, e.g., information about the manager itself, the nodes
  controlled by the manager, and the database schema and compressed data in the cluster.
  - **Remote** - A public interface for interacting with the ModelarDB manager using Apache Arrow Flight.
- [modelardb_server](/crates/modelardb_server) - ModelarDB's DBMS server in the form of the binary `modelardbd`.
  - **Storage** - Manages uncompressed data, compresses uncompressed data, manages compressed data, and writes
  compressed data to Delta Lake.
  - **Configuration** - Manages the configuration of the ModelarDB DBMS server and provides functionality for updating
  the configuration.
  - **Context** - A type that contains all of the components in the ModelarDB DBMS server and makes it easy to share and
  access them.
  - **Data Folders** - A type for managing data and metadata in a local data folder, an Amazon S3 bucket, or an
  Microsoft Azure Blob Storage container.
  - **Error** - Error type used throughout the crate, a single error type is used for simplicity.
  - **Manager** - Manages metadata related to the ModelarDB manager and provides functionality for interacting with the
  ModelarDB manager.
  - **Remote** - A public interface to interact with the ModelarDB DBMS server using Apache Arrow Flight.
- [modelardb_storage](/crates/modelardb_storage) - Library providing functionality for reading from and writing to
storage.
  - **Metadata** - Manages metadata stored in Delta Lake, e.g., information about the tables' schema and compressed
  data.
  - **Optimizer** - Rules for rewriting Apache DataFusion's physical plans for time series tables so aggregates are 
  computed from compressed segments instead of from reconstructed data points.
  - **Query** - Types that implement traits provided by Apache DataFusion so SQL queries can be executed for ModelarDB
  tables.
  - **Delta Lake** - Module providing functionality for reading from and writing to a delta lake.
  - **Error** - Error type used throughout the crate, a single error type is used for simplicity.
  - **Parser** - Extensions to Apache DataFusion's SQL parser. The first extension adds support for creating time series
  tables with a timestamp, one or more fields, and zero or more tags. The second adds support for adding a `INCLUDE
  address[, address+]` clause before `SELECT`.
- [modelardb_test](/crates/modelardb_test) - Library providing functionality for testing ModelarDB.
  - **Data Generation** - Functionality for generating data with a specific structure for use in tests.
  - **Table** - Constants and functionality for testing normal tables and time series tables.
- [modelardb_types](/crates/modelardb_types) - Library of shared macros and types for use by the other crates.
  - **Error** - Error type used throughout the crate, a single error type is used for simplicity.
  - **Functions** - Functions for operating on the types.
  - **Macros** - Macros for extracting an array from a `RecordBatch` and extracting all arrays from a `RecordBatch` with
  compressed segments.
  - **Schemas** - Schemas used throughout the ModelarDB project, e.g., for buffers and for Apache Parquet files with
  compressed segments.
  - **Types** - Types used throughout the ModelarDB project, e.g., for representing timestamps and different kinds of
  error bounds.

## Development
All code must be formatted according to the [Rust Style Guide](https://doc.rust-lang.org/nightly/style-guide/) using
[rustfmt](https://github.com/rust-lang/rustfmt). Subjects not covered in the style guide, or requirements specific to
this repository, are covered here.

### Documentation
All modules must have an accompanying doc comment that describes the general functionality of the module and its
content. Thus, a brief description of the central structs, functions, etc. should be included if important to understand
the module.

Functions and methods should be ordered by visibility and the order in which they are expected to be used. For example,
for a struct its public constructors should be placed first, then the most commonly used public methods, then the 2nd
most commonly used public methods, and so on. Private functions and methods should be placed right after the last public
function or method that calls them.

All public and private structs, traits, functions, and methods must have accompanying doc comments that describe their
purpose. Generally, these doc comments should include a description of the main parameters, the return value, and, if
beneficial, examples.

### Imports
All imports must be grouped such that the first group contains imports from the standard library, the second group
contains imports from external crates, and the third group contains imports from the crate itself. When importing, 
constants and types should be imported directly, e.g., `use modelardb_types::types::TimeSeriesTableMetadata;`, and 
functions should be imported through the module name, e.g., `use modelardb_types::functions;`.

### Terminology
The following terminology must be used throughout the ModelarDB project.

- **maybe** - Used as a prefix for variables of type `Result` or `Option` to indicate it may contain a value.
- **try** - Used as a prefix for functions and methods that return `Result` or `Option` to indicate it may return a
value.
- **normal table** - A relational table that stores data directly in Apache Parquet files managed by Delta Lake and thus
uses the same schema at the logical and physical layer.
- **time series table** - A relational table that stores time series data as compressed segments containing metadata and models
in Apache Parquet files managed by Delta Lake and thus uses different schemas at the logical and physical layer.
- **table** - A normal table or a time series table, e.g., used when a function or method accepts both types of tables.
- **metadata table** - A table that stores metadata in Apache Parquet files managed by Delta Lake, e.g., information about
the tables and the cluster.

### Testing and Linting
All public and private functions must be appropriately covered by unit tests. Full coverage is intended, which means all
branches of computation within each function should be thoroughly tested.

In addition, the following commands must not return any warnings or errors for the code currently in main:
- [cargo build --all-targets](https://doc.rust-lang.org/cargo/commands/cargo-build.html)
- [cargo clippy --all-targets](https://github.com/rust-lang/rust-clippy)
- [cargo doc](https://doc.rust-lang.org/cargo/commands/cargo-doc.html)
- [cargo machete --with-metadata](https://github.com/bnjbvr/cargo-machete)
- [cargo test --all-targets](https://doc.rust-lang.org/cargo/commands/cargo-test.html)

### Crates
To avoid confusion and unnecessary dependencies, a list of crates used in the project is included. Note that this only
includes crates used for purposes such as logging, where multiple crates provide similar functionality.

- Async Runtime - [tokio](https://crates.io/crates/tokio)
- Logging - [tracing](https://crates.io/crates/tracing)
- Memory Allocation - [snmalloc-rs](https://crates.io/crates/snmalloc-rs)
- Object Store Access - [deltalake](https://crates.io/crates/deltalake)
- Property-based Testing - [proptest](https://crates.io/crates/proptest)
- Random Number Generator - [rand](https://crates.io/crates/rand)
- TLS - [rustls](https://crates.io/crates/rustls)
- Temporary Files and Directories - [tempfile](https://crates.io/crates/tempfile)
- UUID - [uuid](https://crates.io/crates/uuid)
- gRPC - [tonic](https://crates.io/crates/tonic)
