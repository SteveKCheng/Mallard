# Mixing row-wise appends with `duckdb_append_data_chunk`

**Question.** With a single DuckDB appender, what happens if you append some values
row-by-row (`duckdb_append_int32` … `duckdb_appender_end_row`) and then also append a whole
chunk through `duckdb_append_data_chunk`?  The official C API docs say nothing about this case.

**Short answer.** DuckDB neither forbids it nor handles it coherently.  There is no error and no
memory corruption, but the rows come out **reordered** (in a way that depends on an internal
2048-row boundary), and mixing *in the middle of a row* silently **drops** the partial row and can
raise a confusingly-timed exception later.  Mallard should treat this combination as illegal.

Findings are from reading DuckDB's own source at the **v1.5.6** release tag (commit `069cc9f`).
The source links below point to that tag on GitHub; line numbers may drift in other versions.

## How the appender buffers data

`BaseAppender` keeps **two** separate buffers — [`appender.hpp:51-55`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/main/appender.hpp#L51-L55):

- `DataChunk chunk` — the *active* buffer for row-wise appends.  `idx_t column` tracks which
  column of the in-progress row is next.
- `ColumnDataCollection collection` — the staging area that is actually committed to the table
  when the appender flushes.

### Row-wise path
`duckdb_append_*` → `BaseAppender::AppendValueInternal` writes into `chunk` at index
`chunk.size()` ([`appender.cpp:86-88`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L86-L88)).  `EndRow` ([`appender.cpp:73-83`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L73-L83)) checks all
columns were supplied, increments the chunk's cardinality, and only moves the chunk into
`collection` once it is **full**:

- `ShouldFlushChunk()` ⇒ `chunk.size() >= STANDARD_VECTOR_SIZE` (2048), [`appender.cpp:760-770`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L760-L770).

So **completed rows below the 2048 boundary stay in `chunk`; they are not yet in `collection`.**

### Chunk path
`duckdb_append_data_chunk` ([`appender-c.cpp:360-367`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/capi/appender-c.cpp#L360-L367)) forwards, with no extra
checks, to `BaseAppender::AppendDataChunk` ([`appender.cpp:348-391`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L348-L391)), which does
`collection->Append(chunk_p)` **directly** (after an optional cast if the types don't match
exactly).  Crucially it **does not call `FlushChunk()` first**, so it never touches the pending
row-wise `chunk`.

### Flush
`Flush()` ([`appender.cpp:404-418`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L404-L418)) calls `FlushChunk()` — which appends the active `chunk` to the
**end** of `collection` ([`appender.cpp:393-402`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L393-L402)) — and then commits the whole `collection` to the
table in collection order via `FlushInternal` (an `INSERT … SELECT` from the collection) and resets
the buffers.

The appender's "cache" (the term the official docs use for what flushing clears) is thus this
two-level buffer: the active `chunk` (≤ `STANDARD_VECTOR_SIZE` = 2048 rows) plus the `collection`.
It is **not** unbounded: after each chunk boundary `ShouldFlush()`
([`appender.cpp:772-782`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L772-L782))
auto-flushes once `collection->Count() >= flush_count`, where `flush_count = DEFAULT_FLUSH_COUNT =
STANDARD_VECTOR_SIZE * 100` = 204 800 rows
([`appender.hpp:33`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/main/appender.hpp#L33)).
A memory-size threshold (`flush_memory_threshold`) also exists but the C API never sets it
(`duckdb_appender_create_ext` leaves it invalid), so through the C API the bound is purely that row
count.  `duckdb_appender_flush` just forces that commit earlier, at a caller-chosen point.

## Consequence 1: rows are reordered

Append N rows row-wise with N < 2048 (so they are still in `chunk`), then call
`duckdb_append_data_chunk` with M rows:

1. The M chunk rows are appended to `collection` **first**.
2. The N row-wise rows remain in `chunk`, and only reach `collection` **later** — at the next
   chunk-fill or at the final flush (close/destroy), where `FlushChunk()` appends them **after**
   the M rows already in `collection`.

Committed order is therefore **[chunk-appended rows] then [earlier row-wise rows]** — reversed
relative to the call order.  Values are all preserved; only the order is wrong.

Worse, the effect is **data-dependent**: any row-wise rows that already crossed a 2048 boundary
were flushed into `collection` at the right place, so only the sub-2048 "tail" gets reordered.
The disorder thus depends on exactly how many rows were appended before the chunk call.

## Consequence 2: mixing mid-row drops data and defers an error

If `duckdb_append_data_chunk` is called after some `duckdb_append_*` calls but **before**
`duckdb_appender_end_row` (so `column != 0`):

- The partially-written row sits at index `chunk.size()` with the cardinality not yet
  incremented.  `AppendDataChunk` ignores it, and the next row-wise append overwrites those slots
  — the partial row's values are **silently discarded**.
- Because the external chunk went straight into `collection`, a later `Flush()` performed while
  `column != 0` throws `InvalidInputException("Failed to Flush appender: incomplete append to
  row!")` ([`appender.cpp:406-408`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L406-L408)) — an error raised far from the actual mistake.

Note that `AppendDataChunk` only triggers its own `Flush()` when `collection` exceeds
`flush_count` ([`DEFAULT_FLUSH_COUNT = STANDARD_VECTOR_SIZE * 100`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/main/appender.hpp#L33) = 204 800 rows), so for ordinary
data sizes nothing surfaces the problem early; it manifests only at the final flush.

## Implication for Mallard

The DuckDB contract for this combination is effectively undefined — ordering hinges on an internal
2048-row boundary and on whether a row is in progress.  Rather than reject the mix, Mallard makes it
**well-ordered by flushing the pending row-wise rows before appending the chunk**:

- Track on `DuckDbAppender` whether any row-wise append has happened since the last flush.
  In `AppendChunk`, when that situation is detected, call `duckdb_appender_flush` first — 
  which commits the buffered row-wise rows to the table — then append the chunk.  
- Only the **row-wise → chunk** transition needs this.  **chunk → row-wise** is already correctly
  ordered: the chunk rows enter `collection` first, and the row-wise rows reach `collection` only at
  the final dispose-flush, i.e. after.  

Caveats:

- **The implicit flush adds a commit boundary.**  `Flush` actually writes the row-wise prefix into
  the table (via `FlushInternal`, committed under the connection's transaction — in autocommit, right
  away).  So a mixed sequence no longer lands all-or-nothing at dispose: the row-wise prefix is
  committed before the chunk is appended, and if a later operation fails, that prefix is already in
  the table.  This is acceptable for an appender (whose commit timing is already loose — see the
  204 800-row auto-flush above) but is a real behavioral change worth documenting for callers.
- **On a flush error, propagate and mark the appender failed** (Mallard's existing `_hasFailed`
  path — dispose only).  This matches how value-set and `FinishRow` errors already behave.

### Why not attempt error recovery via flush?

In v1.5.6 a failed flush does **not** poison the native appender — there is no `flush_failed` flag
anywhere in the source (unlike what the older-version description in [`Appender.md`](Appender.md)
states), and `duckdb_appender_run_function` merely records the error and returns `DuckDBError`
([`appender-c.cpp:108-128`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/capi/appender-c.cpp#L108-L128)).
But recovery is still not practical: `Flush()` resets `collection` only *after* `FlushInternal`
succeeds, so a constraint violation leaves the failed batch sitting in `collection`, and the next
flush re-attempts — and re-fails on — those same rows.  The only escape is `duckdb_appender_clear`
([`appender.cpp:750`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/appender.cpp#L750)), which
discards *all* buffered-uncommitted rows, not selectively.  So for error handling the practical model
is dispose-and-recreate — which `Dispose`/`Close` already do (they flush) — and there is no reason to
expose `duckdb_appender_flush` to clients for recovery.

See [`AppenderChunkWrite.md`](AppenderChunkWrite.md) for the chunk-writing design, and
[`Appender.md`](Appender.md) for the appender overview.
