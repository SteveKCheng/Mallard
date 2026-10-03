# DuckDB appenders and transactions

How DuckDB's appender interacts with transactions, and when constraint violations are detected.
This is DuckDB behavior, not Mallard-specific — it applies to any language binding over the C API.
Findings are from the DuckDB source at the **v1.5.6** release tag (commit `069cc9f`); linked line
numbers may drift in other versions.

See also [`Appender.md`](Appender.md) (overview), [`AppenderChunkWrite.md`](AppenderChunkWrite.md)
(chunk writing), and [`AppenderRowChunkMixing.md`](AppenderRowChunkMixing.md) (buffering and the
row/chunk mixing hazard).

## Two senses of "commit"

The appender docs say flushing "commits" data to the table.  That word is overloaded:

- **Materialize to the table** — move the appender's buffered rows out of its internal buffer and
  into the table's storage, by running an `INSERT`.  This is what `duckdb_appender_flush` /
  `FlushInternal` do.
- **Transaction commit** — make a transaction's changes durable and visible (`COMMIT`).

A flush always does the first.  Whether it also does the second depends entirely on the connection's
transaction state (below).

## An appender shares its connection's transaction

An appender is **not** an independent transactional unit.  The C++ `Appender` keeps a `weak_ptr` to
the **same `ClientContext`** as the `Connection` that created it, and flushing commits through
`ClientContext::Append(stmt)` → `Query(stmt)` — the ordinary query path, not a private transaction
([`client_context.cpp:1308-1313`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/client_context.cpp#L1308-L1313)).
The normal transaction rule then applies: a new transaction is started only when the connection is in
autocommit and none is active
([`client_context.cpp:1248-1251`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/client_context.cpp#L1248-L1251)).

**Autocommit (a fresh connection's default).** Each flush's `INSERT` is wrapped in its own implicit
transaction — committed on success, rolled back on error
([`client_context.cpp:262-285`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/client_context.cpp#L262-L285)).
Flushes are therefore independently durable, and a failed flush rolls back just that batch and leaves
the connection clean.  Note that the appender flushes **implicitly** as it fills (every
`DEFAULT_FLUSH_COUNT` = 204,800 rows — see [`AppenderRowChunkMixing.md`](AppenderRowChunkMixing.md)),
so in autocommit a long append stream is committed to the table in batches *before* you close the
appender, not all at once.

**Inside an explicit transaction** (`BEGIN` issued on the same connection before using the appender).
The appender's flushes run **within that transaction**.  The appended rows become durable only at your
`COMMIT`, and a `ROLLBACK` discards them — including any implicit auto-flushes that fired mid-stream.
This is the genuinely useful property: you can bulk-load through an appender **atomically** as part of
a larger transaction.

Consequences worth stating in binding docs:

- Appended-but-not-yet-flushed rows are **not visible** to other queries (even on the same
  connection) until a flush materializes them.
- The appender and the connection share one context and are single-threaded; interleaving other SQL
  on the same connection with an open appender needs care about flush ordering and visibility.

## When are constraint violations detected?

**At statement (flush) execution — not at `COMMIT`.** DuckDB has **no deferrable/deferred
constraints**: the catalog compatibility views hardcode `is_deferrable = 'NO'` and
`initially_deferred = 'NO'`
([`default_views.cpp:58`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/catalog/default/default_views.cpp#L58)),
and there is no `DEFERRABLE` / `SET CONSTRAINTS` support.  `NOT NULL`, `CHECK`, `PRIMARY KEY` /
`UNIQUE`, and `FOREIGN KEY` are all enforced immediately as the `INSERT` runs.  So a violating flush
fails at flush time; a later `COMMIT` of a transaction whose statements all already succeeded never
newly raises a constraint error — nothing is deferred to re-check.

The one error that genuinely surfaces **at commit** is a **concurrency conflict** under DuckDB's
optimistic MVCC — e.g. two concurrent transactions insert the same unique key, and the second fails
with a write-write / transaction conflict when it commits.  That is optimistic-concurrency
resolution, not a deferred constraint check, and it does not arise for a single appender writing
alone.

## The sharp corner: a constraint violation invalidates the whole transaction

By default a constraint violation invalidates the **entire active transaction**, not just the failing
statement.  `Exception::InvalidatesTransaction`
([`exception.cpp:59-70`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/common/exception.cpp#L59-L70))
returns `false` only for `BINDER`, `CATALOG`, `CONNECTION`, `PARAMETER_NOT_ALLOWED`, `PARSER`, and
`PERMISSION` errors; everything else — **including `CONSTRAINT`** — returns `true`.  So:

- **Autocommit:** the implicit per-flush transaction is rolled back and the connection stays usable
  (you may keep using it, and — see [`AppenderRowChunkMixing.md`](AppenderRowChunkMixing.md) — even
  the native appender object is technically reusable after clearing, though recovery is impractical).
- **Explicit transaction:** the transaction is marked invalid, and every subsequent statement throws
  `"Current transaction is aborted (please ROLLBACK)"`
  ([`error_manager.cpp:19`](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/error_manager.cpp#L19))
  until you `ROLLBACK`.  A flush failure here poisons not just the appender but the user's whole
  transaction.

## Takeaways

- "Flush commits to the table" means a true transaction commit **only in autocommit**; inside an
  explicit transaction, flush merely writes into the open transaction and your `COMMIT` is what makes
  it durable.
- Constraints are checked when rows are inserted (at flush), never deferred to commit; only
  cross-transaction unique conflicts are a commit-time phenomenon.
- Appenders participate in the connection's transaction — great for atomic bulk loads, but it means a
  mid-transaction flush failure (e.g. a constraint violation) aborts the surrounding transaction, not
  only the appender.
- Practical error model: on any append/flush error, stop using the appender (dispose/destroy, which
  flushes); if you were inside an explicit transaction, also `ROLLBACK`.
