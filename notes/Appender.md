# DuckDB Appender in Mallard

This document summarizes findings, architectural patterns, and native API nuances discovered while implementing, analyzing, and testing the DuckDB Appender functionality in Mallard.

---

## 1. Overview & Architectural Design

The DuckDB Appender API is designed for high-throughput bulk row insertion directly into DuckDB tables, bypassing the SQL parser and prepared statement overhead.

In Mallard, the Appender implementation is split into three primary files:

- [`Mallard/Appender/DuckDbAppender.cs`](../Mallard/Appender/DuckDbAppender.cs): Main wrapper around the native `_duckdb_appender*` handle. Manages resource lifecycle, disposal, thread synchronization, and row finalization (`FinishRow`).
- [`Mallard/Appender/DuckDbAppender.Slot.cs`](../Mallard/Appender/DuckDbAppender.Slot.cs): Implements [`ISettableDuckDbValue`](../Mallard/Conversion/ISettableDuckDbValue.cs). Each slot represents an individual column value in the current row being appended.
- [`Mallard/Database/DuckDbConnection.Appender.cs`](../Mallard/Database/DuckDbConnection.Appender.cs): Connection factory methods (`CreateAppender`) supporting default catalog/schema, explicit schema, or explicit catalog and schema.

### Key Mallard Patterns Utilized

1. **`ISettableDuckDbValue` and C# 13 `allows ref struct`**:
   The value-setting API relies on extension methods in [`DuckDbValue`](../Mallard/Conversion/DuckDbValue.cs) parameterized over `TReceiver where TReceiver : ISettableDuckDbValue, allows ref struct`. This avoids boxing allocations when writing values through `ref struct` types like [`DuckDbAppender.Slot`](../Mallard/Appender/DuckDbAppender.Slot.cs).

2. **Sequence Counter (`_sequenceCounter`) Guard**:
   To prevent API misuse (such as reusing a `Slot` instance or writing through stale slots out of order), `DuckDbAppender` tracks an internal `ulong _sequenceCounter`.
   - Each `Slot` captures the parent's current counter upon creation (`Append()`).
   - Prior to passing data to the native DuckDB API, the slot validates that `_parent._sequenceCounter == _sequenceCounter`.
   - On successful append, `_parent._sequenceCounter` increments, immediately invalidating the slot for further writes.

3. **Concurrency Control via `Barricade`**:
   Like connections and statements in Mallard, native calls on the appender are guarded by [`Barricade`](../Mallard/Utilities/Barricade.cs). This enforces single-threaded exclusive execution, disallows accidental recursive re-entrancy from the same thread, and throws `ObjectDisposedException` if operations are attempted on a disposed appender.

---

## 2. DuckDB C API Nuances (`src/include/duckdb.h`)

Analysis of DuckDB's C header (`src/include/duckdb.h`) revealed several specific behaviors:

### Appender Creation
- `duckdb_appender_create_ext(connection, catalog, schema, table, &out_appender)`:
  Passing `NULL` for `catalog` or `schema` instructs DuckDB to use the connection's default catalog and schema. If table resolution fails, it returns `DuckDBError` and sets an error state on the appender.

### Row Finalization
- `duckdb_appender_end_row(appender)`:
  Marks the completion of a row.
  - Returns `duckdb_state` (`DuckDBSuccess` or `DuckDBError`).
  - Can fail if the number of appended columns does not match table schema or if column constraints are violated immediately.

### Destruction vs. Closing vs. Flushing
- `duckdb_appender_flush(appender)`:
  Forces unwritten buffered chunks to be flushed to the table storage without closing the appender.
- `duckdb_appender_clear(appender)`:
  Discards any data that has been appended but not yet flushed to the table.
- `duckdb_appender_close(appender)`:
  Flushes all unwritten data and closes the appender for future writes.
  - **Important Error Diagnostics Nuance**: If flushing triggers a constraint violation, `close` returns `DuckDBError` but keeps the appender allocated so that `duckdb_appender_error(appender)` or `duckdb_appender_error_data(appender)` can be called to retrieve detailed diagnostics.
- `duckdb_appender_destroy(&appender)`:
  Flushes, closes, and deallocates memory.
  - **Caution noted in DuckDB documentation**: If `destroy` triggers a flush error (e.g. constraint violation), it returns `DuckDBError`, but because memory is freed immediately, `duckdb_appender_error` can no longer be inspected. Therefore, calling `duckdb_appender_close` prior to `destroy` is required if detailed error messages are needed.

### Default and Decimal Handling
- `duckdb_append_default(appender)`:
  Appends the column's default value specified in its `DEFAULT` clause, or SQL `NULL` if no default was configured.
- **Absence of `duckdb_append_decimal`**:
  Unlike integers, floats, timestamps, and dates, there is no direct primitive `duckdb_append_decimal` in the C API. Decimals must be created as a generic `duckdb_value` via `duckdb_create_decimal(DuckDbDecimal)` and appended with `duckdb_append_value`.

---

## 3. Mallard Vector Reader & Type System Nuances

During unit test construction, the following reader semantics were verified:

1. **Strict Non-Null Check on `GetItem`**:
   [`DuckDbVectorReader<T>.GetItem(int index)`](../Mallard/Vector/DuckDbVectorReader.cs#L100) enforces `requireValid: true`. Attempting to call `GetItem` on a column whose validity mask bit is 0 throws `InvalidOperationException: The element of the vector at index ... is invalid (null)`.
   - To inspect or read nullable elements safely, callers must use:
     - `IsItemValid(index)`: checks validity mask.
     - `GetItemOrDefault(index)`: returns `T?` (`null` or `default` when invalid).
     - `TryGetItem(index, out T? item)`: boolean check pattern.

2. **Blob Writing**:
   The extension method for writing binary blobs is [`DuckDbValue.SetBlob(ReadOnlySpan<byte>)`](../Mallard/Conversion/DuckDbValue.cs#L377) rather than `Set(...)` to avoid overload ambiguity with other span types.

---

## 4. Test Infrastructure

- **Framework**: Hybrid TUnit execution engine with xUnit v3 assertions.
- **AOT Compatibility**: Tests run under Native AOT analysis and source-generated engine mode.
- **Targeted Execution**:
  - Run via `dotnet test`:
    ```bash
    dotnet test Mallard.Tests/Mallard.Tests.csproj --filter "TestAppender"
    ```
  - Run via TUnit test executable:
    ```bash
    dotnet run --project Mallard.Tests/Mallard.Tests.csproj -- --treenode-filter "/*/*/TestAppender/*"
    ```

---

## 5. Error Handling via `duckdb_error_data`

DuckDB's modern C API uses `duckdb_error_data` as the unified error mechanism. Mallard integrates with it via:
- P/Invokes: `duckdb_destroy_error_data`, `duckdb_error_data_error_type`, `duckdb_error_data_message`, `duckdb_error_data_has_error`, and `duckdb_appender_error_data`.
- Exception helpers in [`DuckDbException`](../Mallard/Basics/DuckDbException.cs):
  - `ThrowForErrorData`: extracts the native message and `DuckDbErrorKind`, cleans up the error data in a `finally` block, and throws.
- `DuckDbAppender` uses `ThrowForAppenderFailure` upon creation failure (e.g. non-existent table reporting `DuckDbErrorKind.Catalog`), on append failure, and in `FinishRow` when `duckdb_appender_end_row` returns `DuckDBError` (e.g. premature end-row reporting `DuckDbErrorKind.InvalidInput`).

---

## 6. Error Handling on `Dispose`/`Close`

See [`notes/DisposeErrors.md`](DisposeErrors.md) for the general
background on why `Dispose` throwing exceptions is usually discouraged, but is an established exception
for write-buffered I/O types (like `Stream`) where silently discarding unflushed data would be worse.
`DuckDbAppender` follows that same reasoning, since disposing it is what actually flushes appended rows
to the table:

- `DuckDbAppender` tracks a private `_hasFailed` flag, set whenever any operation (appending a value,
  or `FinishRow`) reports a DuckDB error. This flag is only ever touched while the appender's `Barricade`
  lock is held (or while `Barricade.PrepareToDisposeOwner` has already claimed exclusive access during
  disposal), so it needs no separate synchronization.
- If `_hasFailed` is already set by the time `Dispose` runs, `Dispose` does **not** attempt to flush or
  report any further error: it just calls `duckdb_appender_destroy` and returns normally. This avoids
  masking the exception that was already thrown for the original failure (e.g. inside a `using` block's
  implicit `finally`).
- Otherwise, `Dispose` calls `duckdb_appender_close` first (which flushes buffered data), captures any
  error via `duckdb_appender_error_data` (while the appender is still alive), *then* calls
  `duckdb_appender_destroy`, and only then throws the captured exception, if any. This matches the nuance
  documented in section 2 above: querying error details after `destroy` is not possible.
- A public `Close()` method is exposed purely as a documented synonym for `Dispose()` (mirroring
  `Stream.Close()`), so callers who want an explicit, obvious point in their code where a flush error
  might surface can call it instead of relying on the implicit `Dispose` at the end of a `using` block.
- Whether called from the finalizer or not, `DisposeImpl` never lets an exception escape unless
  `disposing` is true *and* this is the first error being reported — the finalizer path (`disposing:
  false`) unconditionally skips straight to `duckdb_appender_destroy`.

**Important finding on DuckDB's appender error state (not idempotent):** inspecting DuckDB's own C API
implementation (`AppenderWrapper` in `src/include/duckdb/main/capi/capi_internal.hpp`, and
`src/main/capi/appender-c.cpp`) shows that the appender's stored `ErrorData` is unconditionally
overwritten by each subsequent failing call — there is no "already failed, don't bother trying" guard in
the C++ layer, except for a separate sticky `flush_failed` bool used only by `duckdb_appender_close`/
`duckdb_appender_destroy`. This means that, once an error has occurred, a further append call is not
guaranteed to keep failing (it may spuriously succeed) or to report the *same* error if it does fail; the
original diagnostic information can simply be lost. Because of this, Mallard does **not** rely on DuckDB
to keep reporting a consistent error after the first failure. Instead, `DuckDbAppender.CheckNotFailed`
(called before every attempt to append a value or finish a row) explicitly throws
`InvalidOperationException` once `_hasFailed` is set, without making any further native calls.

## 7. Remaining Potential Enhancements for Future Work

1. **Explicit `Flush()` method**:
   Expose `Flush()` on `DuckDbAppender` for long-running streaming pipelines that require checkpointing
   before disposal. (`Close()` is now implemented; see section 6.)
2. **Appender metadata introspection**:
   Expose `duckdb_appender_column_count` and `duckdb_appender_column_type` to allow callers to verify column counts and types dynamically.  (These two are now imported and used internally by the chunk-writing API — see section 8 — but are not yet surfaced as public members.)

## 8. Chunk-wise (bulk vectorized) appending

Beyond the row-at-a-time API (`Append` / `Slot` / `FinishRow`), the appender supports writing data a
whole *chunk* at a time, straight into the native column-vector memory, via
`DuckDbAppender.AppendChunk`.  This is the write-side mirror of the raw chunk *reader*
(`DuckDbVectorRawReader<T>`), and is the fastest path for bulk inserts.

See [`AppenderChunkWrite.md`](AppenderChunkWrite.md) for the full design.  In brief:

- `appender.AppendChunk(state, (in DuckDbChunkWriter w, state) => { … return rowCount; })` hands the
  callback a `DuckDbChunkWriter` (a `ref struct`, so it cannot escape the callback), which dispenses a
  write-only `DuckDbVectorRawWriter<T>` per column via `GetColumnRaw<T>`.  Write values through its
  `AsSpan()` / indexer / `SetItem`, then return the number of rows populated.
- The reusable native data chunk is created lazily (typed from `duckdb_appender_column_count` /
  `duckdb_appender_column_type`), reset between calls, and destroyed on disposal.
- First cut: "raw" primitive, fixed-width columns only, and every appended row is valid.  Null
  (validity) support, `VARCHAR`/`BLOB`, nested types, and type-converting writers are deferred.
