# Thread safety of the DuckDB C API (what may be shared across threads)

What is safe to touch from multiple threads at once when using DuckDB through its C API, and why.
The short version: **the only things worth sharing across threads are (a) logical types and (b)
read-only, already-materialized result vectors/chunks** — both are semantically immutable, and both are
in fact safe for concurrent *reads*. Everything that mutates (connections, prepared statements,
appenders, chunks being *written*) is single-threaded and must be externally synchronized.

Findings are from the DuckDB source at the **v1.5.6** release tag (commit `069cc9f`); linked line
numbers may drift in other versions. Companion notes: [`ComplexTypeWriting.md`](ComplexTypeWriting.md),
[`AppenderTransactions.md`](AppenderTransactions.md).

## Why this matters for a binding

Two kinds of DuckDB object are naturally shared read-only:

- **Read-only vectors/chunks** — so clients can process a result chunk's columns in parallel.
- **Logical types** — the type-introspection representation a binding exposes (recursive, so it has to
  be built from classes, e.g. a `DuckDbStructColumns`-style tree) is *semantically* read-only; it would
  be silly to force it to be thread-exclusive, and the fixed per-schema type info is the natural thing
  to share among parallel workers.

So the question "is `duckdb_logical_type` safe to query concurrently?" is really "can a binding treat
immutable type/vector info as freely shareable?" — and the answer is yes.

## Logical types: concurrent read-only queries are safe

Querying a `duckdb_logical_type` from multiple threads — including the same handle, and including
queries that descend into children (`duckdb_struct_type_child_type`, `duckdb_list_type_child_type`,
`duckdb_map_type_*`, `duckdb_decimal_*`, …) — is thread-safe. Three facts establish it:

1. **The query functions return a copy, not a mutation.** e.g. `duckdb_struct_type_child_type` /
   `duckdb_list_type_child_type` do `new duckdb::LogicalType(StructType::GetChildType(...))`
   ([`logical_types-c.cpp:256,373`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/capi/logical_types-c.cpp#L256-L383)).
   A `LogicalType` copy copies its `shared_ptr<ExtraTypeInfo>`, so the only shared mutable state touched
   is a **refcount bump** on the child's `ExtraTypeInfo` control block.
2. **That refcount is atomic.** `duckdb::shared_ptr<T>` is a thin wrapper whose backing member is a real
   `std::shared_ptr<T>`
   ([`shared_ptr_ipp.hpp:16`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/common/shared_ptr_ipp.hpp#L16)),
   whose control-block refcount is atomic per the C++ standard. Concurrent copies/destroys of types
   sharing an `ExtraTypeInfo` are therefore race-free.
3. **No lazy mutation on "read".** `LogicalType::physical_type_` is computed eagerly in the constructor
   ([`types.cpp:44-46`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L44-L46));
   `InternalType()` just returns that stored, non-`mutable` field
   ([`types.hpp:267`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/common/types.hpp#L267)).
   The structural payload (`StructTypeInfo::child_types`, the `EnumTypeInfo` dictionary, decimal
   width/scale) is immutable after construction. So a "read" query is genuinely const; concurrent
   readers of the same immutable `ExtraTypeInfo` do not race.

## Read-only vectors/chunks: concurrent reads are safe

A result chunk fetched through the C API is fully **materialized and flat**, and its contents are
immutable for the chunk's lifetime. The read accessors — `duckdb_vector_get_data`,
`duckdb_vector_get_validity`, `duckdb_data_chunk_get_vector`, the nested-child getters
(`duckdb_list_vector_get_child` / `duckdb_struct_vector_get_child` / `duckdb_array_vector_get_child`),
and `duckdb_vector_get_column_type` — are pure reads (the last one additionally *copies* a logical type,
which is the atomic-refcount bump covered above). None of them mutate the chunk on a read path
(allocation only happens on the *write* path, via `duckdb_vector_ensure_validity_writable`). So multiple
threads may read different columns — or the same column — of one chunk concurrently.

## The lifetime rule (the one real hazard)

All of the above is a **read/read** guarantee. Concurrent **read + destroy/mutate of the same object is
not safe** — the ordinary lifetime rule:

- Destroying a shared logical type (`duckdb_destroy_logical_type`) or chunk
  (`duckdb_destroy_data_chunk`) while another thread is still reading it is a data race / use-after-free.
- The owner must outlive all borrowers and be the sole destroyer. A "one designated owner, others
  borrow, owner destroys last" protocol is the correct pattern (this is what Mallard's
  `DuckDbResultChunk.IgnoreDisposals` / reference-count machinery already arranges for readers).

## The performance caveat: atomic-safe ≠ contention-free

"Thread-safe" here means *correct*, achieved via an atomic increment on a shared control block. If a hot
parallel path repeatedly queries child types off one shared logical type, each copy is a contended
atomic read-modify-write on the **same control-block cache line** — classic cache-line bouncing. The fix
is structural, not a lock:

- **Introspect each native logical type once**, build an immutable derived type-info tree on the managed
  side, and share *that* read-only across workers. Pure reads of immutable managed data have zero
  cross-thread contention.
- Don't re-enter the C API per-vector/per-thread in the inner loop. Borrowing the native type is for the
  setup/caching phase; the per-row path should touch only the cached, derived info.

## What is NOT covered (and is not thread-safe to share)

This note is strictly about sharing *immutable, read-only* objects. It does **not** make the mutating
objects safe — these remain single-threaded / externally synchronized:

- **Connections, prepared statements, appenders** — stateful; one thread at a time (Mallard guards these
  with `Barricade` / `HandleRefCount`).
- **Chunks/vectors being *written*** (appender chunk writers, UDF outputs) — mutation; the writer holds
  the vector's validity pointer and (for lists) a size/offset cursor, so a write chunk is owned by one
  thread for the duration of the write.
- **Object destruction** — see the lifetime rule above.
- **The database/transaction** — transaction state is per-connection; see
  [`AppenderTransactions.md`](AppenderTransactions.md).
