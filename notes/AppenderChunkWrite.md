# Design: raw chunk-writing for the DuckDB appender

Status: **implemented** — primitives, now including null/validity support (see §5). All tests in
`Mallard.Tests/TestAppenderChunk.cs` pass.

This note records the design for writing data to a DuckDB appender *by data chunk*
(directly into the memory behind each column vector), as opposed to the existing
row-at-a-time appender API (`DuckDbAppender.Append()` + `Slot` + `FinishRow()`).

It is the write-side mirror of the existing *raw chunk reader*
([`DuckDbVectorRawReader<T>`](../Mallard/Vector/DuckDbVectorRawReader.cs) and friends).
Read that code first; this design deliberately parallels it.

See also [`AppenderFeatures.md`](AppenderFeatures.md) §"Example of appending data in
chunks" for the underlying C pattern, and [`Appender.md`](Appender.md) for the
row-at-a-time appender.

---

## 1. What we are mirroring (the read path)

Reading a result chunk directly off native memory has three layers:

| Layer | Type | Role | Lifetime guard |
|---|---|---|---|
| Owned chunk | `DuckDbResultChunk` (class, `IDisposable`) | owns the native `_duckdb_data_chunk*`, reusable | ref-count / finalizer |
| Scoped accessor | `DuckDbChunkReader` (`readonly ref struct`) | handed to a user callback, dispenses columns | cannot escape the callback |
| Column view | `DuckDbVectorRawReader<T>` (`readonly ref struct`) | raw typed access to one vector's memory | `ref struct`; `T : unmanaged` |

Access is always funnelled through a scoped callback
(`chunk.ProcessContents(state, (in DuckDbChunkReader r, state) => …)`), so native
pointers can never outlive the chunk. `DuckDbVectorRawReader<T>.AsSpan()` returns a
`ReadOnlySpan<T>` straight over `duckdb_vector_get_data`. Type correctness is enforced
at construction by `DuckDbVectorInfo.ValidateElementType<T>(storageKind)`, a static table
mapping each `DuckDbValueKind` to its layout-compatible .NET type(s).

The write path is the mirror image of the **raw reader**: same three layers, same
scoped-callback discipline, same static type-check table, but read methods become write
methods and `ReadOnlySpan<T>` becomes `Span<T>`.

## 2. The native C pattern (from AppenderFeatures.md)

1. build logical types for the columns -> `duckdb_create_data_chunk(types, n)`
2. per column: `duckdb_data_chunk_get_vector` -> `duckdb_vector_get_data` -> write primitives
   directly into the array
3. `duckdb_data_chunk_set_size(chunk, rows)`
4. `duckdb_append_data_chunk(appender, chunk)`
5. `duckdb_data_chunk_reset(chunk)` and reuse

(Null/validity handling via `duckdb_vector_ensure_validity_writable` is also part of the C
pattern and is now supported — see §5.)

In Mallard's interop convention the DuckDB pointer typedefs expand as
`duckdb_data_chunk` -> `_duckdb_data_chunk*`, `duckdb_vector` -> `_duckdb_vector*`,
`duckdb_logical_type` -> `_duckdb_logical_type*`, `duckdb_appender` -> `_duckdb_appender*`.

## 3. New public surface (the minimum)

Three new types plus one method on `DuckDbAppender`, named to parallel the reader:

```csharp
// Analogous to DuckDbChunkReadingFunc<TState, TReturn>.
// Returns the number of rows populated (0 .. Capacity).
public delegate int DuckDbChunkWritingFunc<in TState>(in DuckDbChunkWriter writer, TState state)
    where TState : allows ref struct;

// Analogous to DuckDbChunkReader. Borrows the native chunk for the callback's scope only.
public readonly ref struct DuckDbChunkWriter
{
    public int Capacity { get; }       // = duckdb_vector_size() (2048)
    public int ColumnCount { get; }
    public DuckDbVectorRawWriter<T> GetColumnRaw<T>(int columnIndex)
        where T : unmanaged, allows ref struct;
}

// Analogous to DuckDbVectorRawReader<T>. Write-only: no GetItem/TryGetItem/getter indexer.
public readonly ref struct DuckDbVectorRawWriter<T>
    where T : unmanaged, allows ref struct
{
    public int Capacity { get; }
    public DuckDbColumnInfo ColumnInfo { get; }
    public Span<T> AsSpan();                 // writable span over duckdb_vector_get_data
    public void SetItem(int index, T value);
    // (static) ValidateParamType(valueKind) reusing DuckDbVectorInfo.ValidateElementType<T>
    // NOTE: no SetInvalid / null support in the first cut (see §5).
}
```

Appender entry point, mirroring `DuckDbResultChunk.ProcessContents`:

```csharp
public partial class DuckDbAppender
{
    // Returns the row count that was appended (same value func returned), which is handy
    // for driving a multi-chunk loop without re-deriving the capacity at the call site.
    public int AppendChunk<TState>(TState state, DuckDbChunkWritingFunc<TState> func)
        where TState : allows ref struct;
}
```

Usage reads almost identically to the reader side:

```csharp
using var appender = connection.CreateTableAppender("users");
appender.AppendChunk(data, static (in DuckDbChunkWriter w, MyData d) =>
{
    var id    = w.GetColumnRaw<int>(0).AsSpan();
    var score = w.GetColumnRaw<double>(1).AsSpan();
    for (int i = 0; i < d.Count; i++) { id[i] = d.Ids[i]; score[i] = d.Scores[i]; }
    return d.Count;          // rows written
});
```

## 4. Design decisions (with rationale)

- **Scoped callback, not an exposed writable-chunk object.** Reusing the reader's
  `ref struct` + callback discipline gives dangling-pointer safety for free and keeps the
  public surface tiny. The owned/reusable chunk stays *internal* to the appender.
- **Logical types come from the appender, not the caller.** `duckdb_append_data_chunk`
  requires the chunk's vector types to match the table's columns exactly, so we size and
  type the chunk from `duckdb_appender_column_count` / `duckdb_appender_column_type` rather
  than asking the user to re-declare types (a mismatch waiting to happen). This also
  implements the two introspection functions listed as future work in
  [`Appender.md`](Appender.md) §7.
- **Row count is the delegate's return value**, not a settable property. Returning an `int`
  is compile-time-enforced: omitting it is a compile error, whereas forgetting to set a
  `RowCount` property would only be caught at run time.
- **One call = one chunk** (<= `Capacity`). For > 2048 rows the caller invokes `AppendChunk`
  repeatedly. A looping / `Commit`-style variant is a possible later enhancement.
- **Chunk is cached and reused** inside the appender: created lazily on first `AppendChunk`,
  `duckdb_data_chunk_reset` between calls, destroyed in `DisposeImpl`. Matches the C guidance
  to allocate one chunk and reuse it.
- **Writer is write-only but built on the internal `DuckDbVectorInfo`.** `DuckDbVectorInfo`
  is always `internal`, so reusing it to carry the `_duckdb_vector*` / data pointer / capacity /
  `DuckDbColumnInfo` costs nothing in public-API safety: the *public* read-only vs write-only
  separation is enforced at the `DuckDbVectorRawReader<T>` vs `DuckDbVectorRawWriter<T>` level
  (the writer exposes no read methods). This avoids duplicating the plumbing that the rest of
  Mallard — especially the type converters — already hangs off `DuckDbVectorInfo`. The one new
  primitive added to `DuckDbVectorInfo` is `UnsafeWrite<T>`, the mirror of `UnsafeRead<T>`; the
  `Length` field carries the chunk *capacity* for a write vector.

## 5. Scope of the first cut

**In:** primitive fixed-width types whose .NET layout equals DuckDB storage -- exactly the
`unmanaged` set that `ValidateElementType<T>` already accepts for raw reads: `bool`/`byte`,
the signed/unsigned integers including `Int128`/`UInt128`, `float`, `double`, and the
layout-compatible wrappers `DuckDbDate` / `DuckDbTime` / `DuckDbTimestamp` /
`DuckDbInterval` / `DuckDbUuid`.  Plus **null/validity**: `DuckDbVectorRawWriter<T>.SetInvalid(i)`
marks an element SQL `NULL` (lazily allocating the vector's validity mask via
`duckdb_vector_ensure_validity_writable`).  `duckdb_data_chunk_reset` clears validity masks (and
cardinality) between reuses — confirmed in the header and covered by a test — so each fill starts
all-valid and nulls do not leak across reused chunks.

**Out (explicit non-goals, deferred):**
- **VARCHAR / BLOB** (need `duckdb_vector_assign_string_element_len`; not plain memory writes).
- **Nested LIST / ARRAY / STRUCT**, and **ENUM / DECIMAL** storage nuances.
- **Type-converting ("non-raw") writers** -- would require reverse converters, a separate,
  larger effort.

## 6. Native methods to add (`Interop/NativeMethods.cs`)

```
duckdb_create_data_chunk(_duckdb_logical_type** types, idx_t column_count) -> _duckdb_data_chunk*
duckdb_data_chunk_set_size(_duckdb_data_chunk* chunk, idx_t size) -> void
duckdb_data_chunk_reset(_duckdb_data_chunk* chunk) -> void
duckdb_append_data_chunk(_duckdb_appender* appender, _duckdb_data_chunk* chunk) -> duckdb_state
    // currently stubbed out (commented) with a value-type arg: uncomment + fix signature
duckdb_appender_column_count(_duckdb_appender* appender) -> idx_t
duckdb_appender_column_type(_duckdb_appender* appender, idx_t col_idx) -> _duckdb_logical_type*
    // owned by caller; destroy after duckdb_create_data_chunk copies it
```

Already present and reused: `duckdb_data_chunk_get_vector`, `duckdb_vector_get_data`,
`duckdb_vector_get_validity`, `duckdb_vector_size`, `duckdb_destroy_data_chunk`,
`duckdb_destroy_logical_type`.

For null/validity support, also added: `duckdb_vector_ensure_validity_writable` and
`duckdb_validity_set_row_invalid` (used by `DuckDbVectorInfo.UnsafeSetInvalid`).

## 7. Safety analysis (mirrors reader guarantees)

- **No dangling pointers:** writer types are `ref struct`s reachable only inside the
  `AppendChunk` callback; `TState : allows ref struct` but the delegate returns a plain
  `int`, so nothing pointer-bearing escapes.
- **Thread-exclusivity:** `AppendChunk` holds the appender's `Barricade` for its whole
  duration (like `FinishRow`), so chunk writes cannot race row writes or disposal.
- **Default-struct safety:** a zero-initialized `DuckDbVectorRawWriter<T>` has a null data
  pointer -> writes fault with `NullReferenceException`, per the coding-style rule.
- **Type correctness:** `GetColumnRaw<T>` validates `T` against the column's `StorageKind`
  via the shared table before returning a writer; wrong `T` throws `ArgumentException`,
  exactly as the raw reader does.
- **Bounds:** `SetItem` / `AsSpan` bound against `Capacity`; the returned row count is
  validated to `0 .. Capacity` before `duckdb_data_chunk_set_size`.
- **Interaction with the Slot API:** bump `_sequenceCounter` at the start of `AppendChunk`
  so any outstanding `Slot` from the row-wise API is invalidated across a chunk write.

## 8. Test plan (`Mallard.Tests/TestAppender.cs`)

Mirror the existing `BasicAppend` round-trip style: append via `AppendChunk`, then read back
with `GetColumnRaw` / `GetColumn` and assert. Cases:
- single full chunk;
- partial chunk (rows < capacity);
- multiple chunks (> 2048 rows) exercising reset/reuse;
- wrong-`T` throws `ArgumentException`;
- returned row count out of range throws;
- an all-primitive-types table matching the reader's type matrix.

Null/validity cases: a column with interleaved nulls round-tripped back (via `IsItemValid` /
`GetItemOrDefault`), and a null written in a reused chunk not leaking into the next fill.

## 9. Work breakdown / files touched (as implemented)

1. `Interop/NativeMethods.cs` -- added `duckdb_create_data_chunk`, `duckdb_data_chunk_set_size`,
   `duckdb_data_chunk_reset`, `duckdb_appender_column_count`, `duckdb_appender_column_type`, and
   enabled `duckdb_append_data_chunk` (was commented out; fixed its arg to `_duckdb_data_chunk*`);
   plus `duckdb_vector_ensure_validity_writable` and `duckdb_validity_set_row_invalid` for nulls.
2. `Mallard/Vector/DuckDbVectorInfo.cs` -- added `UnsafeWrite<T>` (mirror of `UnsafeRead<T>`) and
   `UnsafeSetInvalid` (ensures a writable validity mask, then clears the row's validity bit).
3. `Mallard/Vector/DuckDbVectorRawWriter.cs` -- new write-only `ref struct` wrapping
   `DuckDbVectorInfo`, with `SetItem` / indexer and `SetInvalid`; `AsSpan` added in
   `DuckDbVectorMethods.RawWriter.cs` to parallel `DuckDbVectorMethods.RawReader.cs`.
4. `Mallard/Appender/DuckDbChunkWriter.cs` -- new `ref struct` + the `DuckDbChunkWritingFunc`
   delegate.
5. `Mallard/Appender/DuckDbAppender.Chunk.cs` -- new `partial`: cached write-chunk +
   logical-type lifecycle, `AppendChunk`; teardown wired into `DisposeImpl` in
   `DuckDbAppender.cs`.
6. `Mallard/Vector/DuckDbVectorRawReader.cs` -- disambiguated an `AsSpan` `<see cref>` that the
   new overload made ambiguous (CS0419).
7. `Mallard.Tests/TestAppenderChunk.cs` -- new test class with the cases in §8 (kept separate
   from the large `TestAppender.cs`).
8. `Appender.md` §7 -- the `duckdb_appender_column_count` / `duckdb_appender_column_type`
   introspection item is now realized (used internally to type the write chunk).
