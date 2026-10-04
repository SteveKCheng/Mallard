# Writing complex (nested) DuckDB types to vectors

Background and DuckDB semantics for writing the nested/composite types — `LIST`, `ARRAY`, `STRUCT`,
`MAP`, `UNION` — and `DECIMAL`, to data-chunk vectors (appender, and the UDF output surfaces).  This
is DuckDB behavior relevant to any C-API binding, plus the design implications for Mallard's writers.

Findings are from the DuckDB source at the **v1.5.6** release tag (commit `069cc9f`); linked line
numbers may drift in other versions.  Companion notes: [`AppenderChunkWrite.md`](AppenderChunkWrite.md)
(raw chunk writing), [`AppenderRowChunkMixing.md`](AppenderRowChunkMixing.md),
[`AppenderTransactions.md`](AppenderTransactions.md).

## Physical representations

A DuckDB vector of a nested type is a tree of vectors.  There are **no** vector-level MAP or UNION
accessors in the C API (the `duckdb_get_map_*` functions work on scalar `duckdb_value`s, not vectors);
you read/write MAP and UNION through their physical `LIST`/`STRUCT` form.

- **`STRUCT`** — one child vector per field, each the same logical size as the parent; retrieve with
  `duckdb_struct_vector_get_child(v, i)`.  The struct vector itself has **no data buffer** (do not call
  `duckdb_vector_get_data` on it); it carries only a validity mask plus its children.
- **`LIST`** — the list vector's own data is an array of `duckdb_list_entry { uint64 offset, length }`
  (Mallard's `DuckDbListRef`), one per row, plus a validity mask.  The elements live in a separate,
  **flattened child vector** (`duckdb_list_vector_get_child(v)`) whose length is dynamic (see "List
  reservation" below).  A row's entry gives the `[offset, offset+length)` slice of that child.
- **`ARRAY`** — fixed length `N`.  No per-row entry and no sizing: the child vector
  (`duckdb_array_vector_get_child(v)`) has size exactly `parent_capacity * N`; element `j` of row `i`
  is at child index `i*N + j`.
- **`MAP` = `LIST(STRUCT("key" K, "value" V))`** ([`types.cpp:1648-1652`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L1648-L1652)).
  The child names are forced to exactly `"key"`/`"value"`.  DuckDB requires map **keys be non-NULL and
  unique within a row**.  Writing a MAP = writing a LIST whose child is a 2-field STRUCT.
- **`UNION` = `STRUCT("tag" UTINYINT, member0, member1, …)`** — a hidden `UTINYINT` `tag` field is
  inserted at position 0 ([`types.cpp:1670-1674`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L1670-L1674)),
  so `duckdb_union_type_member_type(t, i)` corresponds to struct child `i+1`.  Per row the `tag` holds
  the active member's index and the inactive members are set NULL.  Max 256 members (fits the tag).
- **`DECIMAL`** — the significand is stored as a **fixed-width two's-complement integer** (the value as
  if there were no decimal point); the scale lives in the logical type.  The physical integer width is
  chosen by precision ([`types.cpp:104-115`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L104-L115)):

  | precision (width) | storage |
  |---|---|
  | 1–4 | `INT16` (`short`) |
  | 5–9 | `INT32` (`int`) |
  | 10–18 | `INT64` (`long`) |
  | 19–38 | `INT128` (`Int128`) |

  `duckdb_decimal_internal_type` returns that storage type; `duckdb_decimal_width` / `_scale` the rest.
  **Type-check correctness:** the valid raw element type is not a free choice among the four — it must
  equal the *width-derived* storage type, i.e. check against `StorageKind` (from
  `duckdb_decimal_internal_type`), never `ValueKind` (which is always `Decimal`).

## Validity masks in nested types

**Every vector — parent and each child, recursively — has its own, independent validity mask**
(`duckdb_vector_get_validity` / `duckdb_vector_ensure_validity_writable` operate per vector).

- **`STRUCT`:** the struct vector's mask marks the *whole struct* NULL at a row; each child has its own
  mask for field-level nullness.
- **`LIST`:** the list vector's mask marks a *list entry* NULL — distinct from an **empty list** (valid,
  `length == 0`).  The flattened child vector has its own mask for element nullness.
- **`ARRAY`:** the array vector's mask marks the whole array NULL; elements are in the fixed-stride child.
- **`MAP` / `UNION`:** inherit the above via their list/struct form; for UNION the `tag` additionally
  selects which member is live per row.

### The STRUCT "parent NULL ⇒ children NULL" invariant — and why it is less scary than it looks

DuckDB expects that **for any NULL entry in a struct, each child must also be NULL at that row**
([`vector.cpp:1833-1841`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/vector.cpp#L1833-L1841)).
At first glance this is alarming because it is enforced with a hard `D_ASSERT` (an `abort()`, not a
C++ exception).  But two facts defuse it for a binding that links the **official release** library:

1. The enclosing `Vector::Verify` is compiled only in debug builds — its entire body is under
   `#ifdef DEBUG` ([`vector.cpp:1667`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/vector.cpp#L1667)).
2. In a non-debug build `D_ASSERT` reduces to `assert`, which `NDEBUG` turns into a no-op
   ([`assert.hpp:13-40`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/common/assert.hpp#L13-L40)).

So in a release DuckDB (what ships, and what Mallard links — `v1.5.6 (Variegata)` is a release build)
the check is **not compiled and not run**: violating the invariant will not `abort()`, and there is no
higher-level path that throws for it either (searched; none).  The behavior is merely *ill-specified*:
in practice readback is benign because a NULL parent masks its children regardless of their values, but
that is not a guarantee across operators or versions.  A DuckDB built with `DEBUG` (or
`DUCKDB_FORCE_ASSERT`) **would** abort.

**Implication for Mallard.** We do **not** need to cripple the public validity-mask span API to avoid a
production crash — the span stays usable for `STRUCT` as for any vector.  Instead:

- Raw struct writers: document that the caller must uphold parent-NULL ⇒ children-NULL (consistent with
  the general "raw writer = caller knows what they're doing" contract), and note that only a debug
  DuckDB would abort on a violation.
- The future non-raw / high-level struct writer should uphold it automatically (when it sets a struct
  row NULL, recursively NULL the children).
- Restricting the validity span specifically for `STRUCT` is an available *defensive* option if we ever
  decide the debug-build footgun is worth closing, but it is not required for release-build safety.

## List reservation (the two gotchas)

Writing a `LIST` (or `MAP`) column requires growing its flattened child vector explicitly. The protocol:

1. `duckdb_list_vector_reserve(listVec, totalChildElements)` — ensure capacity.
2. `duckdb_list_vector_get_child(listVec)` then `duckdb_vector_get_data(child)` — **after** reserving.
3. Write the child elements `[0, total)`, and write each row's `duckdb_list_entry{offset, length}` into
   the list vector's own data (cumulative `offset` into the child).
4. `duckdb_list_vector_set_size(listVec, total)` — mark how many child elements are live.
5. Set the list vector's validity for any NULL list entries.

Two sharp edges, both from the C++ impl
([`data_chunk-c.cpp:198-214`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/capi/data_chunk-c.cpp#L198-L214),
[`vector.cpp:2524`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/vector.cpp#L2524)):

- **`reserve` may reallocate the child buffer** (it grows capacity to the next power of two). So any
  child data pointer obtained from `duckdb_vector_get_data` **before** a reserve is invalidated — always
  (re-)fetch the child and its data pointer *after* reserving. If sizes are not known up front and you
  reserve incrementally, re-fetch each time.
- **`set_size` does not reserve** and will happily set a size exceeding capacity — which is silent
  memory corruption on write. Always `reserve >= size` first. (`set_size` is in *child elements*, i.e.
  the total across all rows, not a row count.)

`ARRAY` needs none of this (fixed stride, child pre-sized at `parent_capacity * N`); `STRUCT` needs none
(children track the parent size).

## C API inventory for nested writing

- Navigation: `duckdb_struct_vector_get_child(v,i)`, `duckdb_list_vector_get_child(v)`,
  `duckdb_array_vector_get_child(v)`.
- List sizing: `duckdb_list_vector_reserve`, `duckdb_list_vector_set_size`, `duckdb_list_vector_get_size`.
- Type introspection: `duckdb_list_type_child_type`, `duckdb_array_type_child_type` /
  `duckdb_array_type_array_size`, `duckdb_struct_type_child_count` / `_child_name` / `_child_type`,
  `duckdb_map_type_key_type` / `_value_type`, `duckdb_union_type_member_count` / `_member_name` /
  `_member_type`, `duckdb_decimal_width` / `_scale` / `_internal_type`.
- Per-vector validity: `duckdb_vector_get_validity`, `duckdb_vector_ensure_validity_writable`.

## Design implications for Mallard

- The write-side needs the recursive mirror of the reader's child accessors on
  `DuckDbVectorRawWriter<T>`: struct-child, list-child (+ reserve/set_size + a running offset cursor),
  array-child. `MAP` and `UNION` then compose from list+struct (plus the UTINYINT tag convention).
- The list offset cursor is **stateful**, which sits awkwardly with today's immutable
  `readonly ref struct` writer — list building likely needs a small mutable cursor (unlike reading,
  where offsets are given, not accumulated).
- These same primitives serve every vector-writing surface in the C API (appender, and the scalar /
  table / aggregate-finalize / cast UDF outputs — see the survey in the project discussion), so getting
  the raw nested-writer API right is the shared substrate; "non-raw" (type-converting) writers layer on
  top by driving the raw writer through reverse `VectorElementConverter`s.
- Open item to verify when implementing: the required ordering of `list_vector_reserve` vs child writes
  vs `set_size` under all paths, since mistakes there surface as corruption, not an assert, in release.
