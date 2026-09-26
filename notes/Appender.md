# DuckDB Appender in Mallard

This document summarizes findings, architectural patterns, and native API nuances discovered while implementing, analyzing, and testing the DuckDB Appender functionality in Mallard.

---

## 1. Overview & Architectural Design

The DuckDB Appender API is designed for high-throughput bulk row insertion directly into DuckDB tables, bypassing the SQL parser and prepared statement overhead.

In Mallard, the Appender implementation is split into three primary files:

- [`Mallard/Appender/DuckDbAppender.cs`](file:///home/steve/dev/Mallard/Mallard/Appender/DuckDbAppender.cs): Main wrapper around the native `_duckdb_appender*` handle. Manages resource lifecycle, disposal, thread synchronization, and row finalization (`FinishRow`).
- [`Mallard/Appender/DuckDbAppender.Slot.cs`](file:///home/steve/dev/Mallard/Mallard/Appender/DuckDbAppender.Slot.cs): Implements [`ISettableDuckDbValue`](file:///home/steve/dev/Mallard/Mallard/Conversion/ISettableDuckDbValue.cs). Each slot represents an individual column value in the current row being appended.
- [`Mallard/Database/DuckDbConnection.Appender.cs`](file:///home/steve/dev/Mallard/Mallard/Database/DuckDbConnection.Appender.cs): Connection factory methods (`CreateAppender`) supporting default catalog/schema, explicit schema, or explicit catalog and schema.

### Key Mallard Patterns Utilized

1. **`ISettableDuckDbValue` and C# 13 `allows ref struct`**:
   The value-setting API relies on extension methods in [`DuckDbValue`](file:///home/steve/dev/Mallard/Mallard/Conversion/DuckDbValue.cs) parameterized over `TReceiver where TReceiver : ISettableDuckDbValue, allows ref struct`. This avoids boxing allocations when writing values through `ref struct` types like [`DuckDbAppender.Slot`](file:///home/steve/dev/Mallard/Mallard/Appender/DuckDbAppender.Slot.cs).

2. **Sequence Counter (`_sequenceCounter`) Guard**:
   To prevent API misuse (such as reusing a `Slot` instance or writing through stale slots out of order), `DuckDbAppender` tracks an internal `ulong _sequenceCounter`.
   - Each `Slot` captures the parent's current counter upon creation (`Append()`).
   - Prior to passing data to the native DuckDB API, the slot validates that `_parent._sequenceCounter == _sequenceCounter`.
   - On successful append, `_parent._sequenceCounter` increments, immediately invalidating the slot for further writes.

3. **Concurrency Control via `Barricade`**:
   Like connections and statements in Mallard, native calls on the appender are guarded by [`Barricade`](file:///home/steve/dev/Mallard/Mallard/Utilities/Barricade.cs). This enforces single-threaded exclusive execution, disallows accidental recursive re-entrancy from the same thread, and throws `ObjectDisposedException` if operations are attempted on a disposed appender.

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
   [`DuckDbVectorReader<T>.GetItem(int index)`](file:///home/steve/dev/Mallard/Mallard/Vector/DuckDbVectorReader.cs#L100) enforces `requireValid: true`. Attempting to call `GetItem` on a column whose validity mask bit is 0 throws `InvalidOperationException: The element of the vector at index ... is invalid (null)`.
   - To inspect or read nullable elements safely, callers must use:
     - `IsItemValid(index)`: checks validity mask.
     - `GetItemOrDefault(index)`: returns `T?` (`null` or `default` when invalid).
     - `TryGetItem(index, out T? item)`: boolean check pattern.

2. **Blob Writing**:
   The extension method for writing binary blobs is [`DuckDbValue.SetBlob(ReadOnlySpan<byte>)`](file:///home/steve/dev/Mallard/Mallard/Conversion/DuckDbValue.cs#L377) rather than `Set(...)` to avoid overload ambiguity with other span types.

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

## 5. Potential Enhancements for Future Work

1. **Check `duckdb_appender_end_row` status**:
   Ensure `FinishRow()` invokes `ThrowOnAppendFailure(NativeMethods.duckdb_appender_end_row(_nativeObj))` so premature row completion or type errors fail fast.
2. **Native error extraction**:
   Call `duckdb_appender_error(_nativeObj)` inside `ThrowOnAppendFailure` to surface descriptive native messages rather than generic exceptions.
3. **Explicit `Flush()` and `Close()` methods**:
   Expose `Flush()` and `Close()` on `DuckDbAppender` for long-running streaming pipelines that require checkpointing before disposal.
4. **Appender metadata introspection**:
   Expose `duckdb_appender_column_count` and `duckdb_appender_column_type` to allow callers to verify column counts and types dynamically.
