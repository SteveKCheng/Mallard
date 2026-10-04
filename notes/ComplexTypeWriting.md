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

## Nested lists, and what list-entry offsets may contain

**`LIST(LIST(...))` is supported** (arbitrary nesting), and it is nested **per level, not globally
flattened**.  `LIST(LIST(INT))` is three stacked vectors: the outer list vector (its own
`list_entry{offset,length}` + validity) whose child is *another* list vector (its own `list_entry`
array + validity + its own child size) whose child is a flat `INT` vector.  Each list level has an
independent `(offset,length)` indirection and its own `get_child` / `reserve` / `set_size`.  So writing
`LIST(LIST(T))` is a list writer whose child is a list writer whose child is a `T` writer, and the
reserve-once sizing (below) applies at **each** level.

**The entries need not be contiguous, ordered, or disjoint.**  When a list vector is materialized
(which the appender flush does — `VectorOperations::Copy`,
[`vector_copy.cpp:224-242`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/vector_operations/vector_copy.cpp#L224-L242)),
DuckDB processes each row independently by its own `(offset, length)`, gathers exactly
`offset+0 … offset+length-1` from the child via a selection vector, and *compacts* them into a fresh
contiguous child in the target.  Therefore:

- **Gaps** (child elements no row references) are silently dropped — harmless, just wasted space.
- **Out-of-order** (row 1's slice physically after row 2's) is honored; the copy re-lays-out into
  logical row order.
- **Overlap** (two rows reference the same child range) duplicates those elements into both lists —
  well-defined, not undefined behavior (just rarely intended).

**The one hard requirement:** every entry must satisfy `offset + length <= childSize` (the size set via
`duckdb_list_vector_set_size`), and those child slots must have been written.  DuckDB only checks this
in a **DEBUG** build (`D_ASSERT(le->offset + le->length <= ListVector::GetListSize(...))`,
[`vector.cpp:1860`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/vector.cpp#L1860));
the release copy path reads `offset+j` with no bounds check, so an entry running past the child size is
an out-of-bounds read in release — garbage data or memory corruption.  **This is the sole list-offset
invariant Mallard must guarantee.**

This is convenient for the writer design: the client may compute offsets freely (sparse, unordered),
and Mallard only has to (a) reserve + `set_size` the child to cover the maximum `offset+length` used and
(b) bound-check each entry against that declared child size.  Because `reserve` can realloc and C# has
no way to retract a `Span<T>` already handed out, Mallard's contract is **declare the child size up
front, reserve once, then it is fixed** — per list level.

## C API inventory for nested writing

- Navigation: `duckdb_struct_vector_get_child(v,i)`, `duckdb_list_vector_get_child(v)`,
  `duckdb_array_vector_get_child(v)`.
- List sizing: `duckdb_list_vector_reserve`, `duckdb_list_vector_set_size`, `duckdb_list_vector_get_size`.
- Type introspection: `duckdb_list_type_child_type`, `duckdb_array_type_child_type` /
  `duckdb_array_type_array_size`, `duckdb_struct_type_child_count` / `_child_name` / `_child_type`,
  `duckdb_map_type_key_type` / `_value_type`, `duckdb_union_type_member_count` / `_member_name` /
  `_member_type`, `duckdb_decimal_width` / `_scale` / `_internal_type`.
- Per-vector validity: `duckdb_vector_get_validity`, `duckdb_vector_ensure_validity_writable`.

## Scalar values and type equality (mixed nominal / structural)

This section is about the *scalar* `duckdb_value` API (prepared-statement parameters, appender row
values) rather than vectors, but the typing rules it documents apply throughout DuckDB — they are
useful to know even as a plain SQL user.

### Use the dedicated MAP/UNION value creators

Although MAP and UNION are physically backed by STRUCT (see "Physical representations"), a scalar MAP or
UNION value is **not** built with `duckdb_create_struct_value` — a struct value has
`LogicalTypeId::STRUCT`, which is not equal to MAP or UNION (see below) and there is no implicit
struct→map/union coercion. DuckDB provides dedicated creators (stable as of v1.5.6):

- `duckdb_create_map_value(map_type, duckdb_value *keys, duckdb_value *values, idx_t entry_count)` —
  parallel key/value arrays, not pre-assembled key/value structs.
- `duckdb_create_union_value(union_type, idx_t tag_index, duckdb_value value)` — the active member's
  tag index plus that member's value.

(The companions `duckdb_create_struct_value`, `duckdb_create_list_value`, `duckdb_create_array_value`,
and `duckdb_create_enum_value` cover the other composites.)

### How DuckDB compares logical types

`LogicalType::operator==` is `id_ == rhs.id_ && EqualTypeInfo(rhs)`
([`types.cpp:2071`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L2071)), and
`EqualTypeInfo` → `ExtraTypeInfo::Equals` compares the **alias**, the **extension metadata**, and then
the structural content
([`extra_type_info.cpp:112-146`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/extra_type_info.cpp#L112-L146)).
So equality is **structural within a matching type id, but also alias- and ENUM-sensitive**:

- **Type id must match.** A `STRUCT` never equals a `MAP` or `UNION`, even with identical children —
  this is the nominal part, and why a struct value can't stand in for a map/union.
- **Within the id, structural.** `STRUCT` compares its child `(name, type)` pairs *including order*
  (`child_types == other.child_types`); `MAP` its key/value types; `UNION` its members; `DECIMAL` its
  width + scale; `LIST`/`ARRAY` their child type (+ array size).
- **Alias / extension sensitive.** A type carrying an alias (a `CREATE TYPE` user-defined type, or an
  extension type) is only equal to another with the *same* alias; an anonymous rebuilt type will not
  match an aliased one.
- **ENUM is nominal-by-content.** Two enums are equal only if their dictionaries match exactly — same
  size and same value strings in the same insert order
  ([`extra_type_info.cpp` `EnumTypeInfo::EqualsInternal`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/extra_type_info.cpp)).

### Consequence: when you may rebuild a type vs. must reuse the schema's

- For **plain anonymous composites** (`STRUCT`/`LIST`/`ARRAY`/`MAP`/`UNION`/`DECIMAL`, no alias), a
  freshly materialized, structurally-identical type (`duckdb_create_map_type`, `_struct_type`, …)
  compares equal to the column's — you need not thread through the exact schema object.
- You **must** reuse (or faithfully replicate) the schema's type when it carries an **alias** / is a
  user-defined or extension type, or is/contains an **ENUM**, or simply to avoid getting nested names
  and order exactly right by hand.
- Binding and appending additionally apply **implicit casts** to the target type, so a
  not-quite-equal-but-castable value can still succeed — but don't rely on that for composites (there
  is no implicit struct→map, and a failed cast is a runtime error).

The target type is readily available in both scalar-write contexts, so reusing it is the easy *and*
safe default: appender column → `duckdb_appender_column_type(appender, col)`; prepared-statement
parameter → `duckdb_param_logical_type(stmt, idx)`. Derive nested member types from it with
`duckdb_map_type_key_type` / `_value_type`, `duckdb_union_type_member_type`,
`duckdb_struct_type_child_type`, etc.

## Aliases (and why they force keeping the native logical type)

DuckDB's "alias" is a non-standard use of the word: rather than an *alternate* name for a type, it is a
name that becomes part of the type's **identity** and is compared in type equality (see "How DuckDB
compares logical types" above). Key facts (v1.5.6):

- **It is a single `string alias` on the base `ExtraTypeInfo` — at most one** (there is no notion of
  multiple aliases). `LogicalType::SetAlias` attaches a `GENERIC_TYPE_INFO` to carry it when the type has
  no `ExtraTypeInfo` yet
  ([`types.cpp:1431`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/types/types.cpp#L1431)).
- **Any type can carry one, including simple ones** — it is *not* restricted to recursive/complex types.
  The headline example is `JSON`: physically a `VARCHAR` that carries `alias = "JSON"`. So a simple
  physical type can still be a distinct DuckDB type by virtue of its alias.
- **It participates in equality and is surfaced first by `ToString`.** `LogicalType::ToString()` returns
  the alias (plus any extension modifiers) when present, before the structural spelling. For `JSON`,
  whose structure is identical to `VARCHAR`, the alias is the *sole* discriminator: `JSON != VARCHAR`
  under `operator==` even though both are physically `VARCHAR`.

**Does the alias reach the column types a binding introspects?** Sometimes — and that is the catch:

- **`CREATE TYPE foo AS <base/list/enum/struct>`**: in v1.5.6 the alias is **resolved away** when `foo`
  is used as a column type. Such a column's logical type is just the underlying structural type
  (`myint` → `INTEGER`, `mylist` → `INTEGER[]`, a named `STRUCT`/`ENUM` → its structural spelling).
  Confirmed empirically: `duckdb_columns().data_type` shows the structural form, and because `ToString`
  would have shown the alias if present, the alias field really is empty.
- **Extension / built-in named types such as `JSON`** (and spatial `GEOMETRY`, etc.): the alias **is**
  the type's identity and **survives** into the column's logical type. A `JSON` column comes back from
  `duckdb_column_logical_type` / `duckdb_appender_column_type` as `VARCHAR` with `alias = "JSON"`.

**Design rule for Mallard.** Because an alias can ride on a *simple* type (JSON), the argument for
preserving the originating `duckdb_logical_type` is **broader than "complex/recursive types"**: retain
the native handle whenever a column's type has an alias or extension info, in addition to the
nested-type cases. Rebuilding a type from `(id + children)` alone silently drops the alias — turning
`JSON` into `VARCHAR`, which is structurally equal to a rebuilt `VARCHAR` but *unequal* to the real
`JSON` under DuckDB's `operator==`, i.e. a different type identity.

Two things keep the cost of this bounded:

- On the **write** path DuckDB applies implicit casts (`VARCHAR` → `JSON` is implicit), so appending a
  string to a `JSON` column — or a query-appender whose virtual column is declared `VARCHAR` — usually
  still succeeds by coercion. So the alias matters mainly for *faithful type identity / introspection*
  and for cases where no implicit cast exists, not for breaking ordinary appends.
- Retaining the native type is no longer an ownership headache: per
  [`DuckDbThreadSafety.md`](DuckDbThreadSafety.md), logical types are immutable and cheap/safe to share,
  which removes the original reason Mallard avoided storing `duckdb_logical_type`.

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
