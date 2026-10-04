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
3. **No lazy mutation on "read" — neither in `LogicalType` nor in `ExtraTypeInfo`.**
   `LogicalType::physical_type_` is computed eagerly in the constructor
   ([`types.cpp:44-46`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L44-L46));
   `InternalType()` just returns that stored, non-`mutable` field
   ([`types.hpp:267`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/common/types.hpp#L267)).
   The shared payload behind the `shared_ptr<ExtraTypeInfo>` is likewise built at construction and never
   mutated on a read path: there is no `mutable` field, no lazy cache, and no `mutex`/`atomic`/lock in
   `extra_type_info.hpp`/`.cpp` or its subclasses. The usual lazy-cache suspect — the ENUM string→index
   lookup map — is in fact populated eagerly in the `EnumTypeInfoTemplated` constructor, and `GetValues()`
   is `const`
   ([`enum_type_info.hpp:11-44`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/common/extra_type_info/enum_type_info.hpp#L11-L44));
   `StructTypeInfo::child_types`, list/array child types, and decimal width/scale are set once in their
   constructors too.

   Note that `LogicalType` holds `shared_ptr<ExtraTypeInfo>`, *not* `shared_ptr<const ExtraTypeInfo>` — so
   the immutability is by construction/discipline, not enforced by the type system. That is a deliberate
   DuckDB convention (they don't `const`-qualify the payload), not a sign of post-construction mutation;
   the absence of any mutation path is what was verified above. There is correspondingly **no internal
   lock**, because none is needed — immutable-after-construction is the stronger, lock-free guarantee.
   This is the same property DuckDB's own executor relies on when it shares one `LogicalType` /
   `ExtraTypeInfo` across operator threads by `const&` / `shared_ptr` instead of cloning per thread, so
   DuckDB's internal multithreading corroborates the guarantee.

   (Scope: this covers the resolved data types a binding actually sees in result/appender columns.
   A not-yet-bound placeholder such as `UnboundTypeInfo` is a pre-binding artifact and does not appear
   there.)

### The one exception: `SetAlias` mutates in place

There is exactly one mutator on a logical type: `LogicalType::SetAlias`
(`duckdb_logical_type_set_alias`). It is **not** copy-on-write — it writes `type_info_->alias` directly
into the existing, `shared_ptr`-shared `ExtraTypeInfo` (or allocates a fresh `GENERIC_TYPE_INFO` when the
type has none yet)
([`types.cpp:1431`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L1431)).
So calling it on a type whose `ExtraTypeInfo` is shared mutates every `LogicalType` sharing that info and
races any concurrent reader. (A direct consequence: you cannot "rename a copy" — `SetAlias` on a copy
also changes the source, because the copy shares the info.)

This does **not** undermine the read guarantee above, because nothing mutates a *published/shared* type:
DuckDB's own three `LogicalType::SetAlias` call sites are all type-definition/DDL-time on unpublished
instances — the `JSON` and `GEOMETRY` factories call it on a brand-new `VARCHAR`/`BLOB` (null
`type_info_`, so a fresh exclusively-owned info is allocated;
[`types.cpp:1756`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L1756)), and
the catalog `ToSQL` path mutates a copy only during single-threaded DDL string generation. Types that
reach parallel readers (result / column / appender types) are fully built, aliases included, before
being shared, and are never mutated thereafter.

**Rule for Mallard:** treat any shared logical type as strictly read-only and never call `set_alias` on
it (there is no need to expose it). If a type ever has to be aliased, do it on a freshly created type
with no shared `type_info_`, never on a borrowed or copied handle.

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
