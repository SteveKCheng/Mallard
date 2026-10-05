---
title: Examples
description: Worked examples for Mallard, extracted from its unit tests.
---

# Examples

Every snippet on this page is extracted verbatim, at documentation build time, from
[`Mallard.Tests/DocExamples.cs`][src]. They are ordinary
[TUnit](https://tunit.dev/) tests: they compile against the real library and run in CI.
An example that stops working breaks the build rather than quietly going stale on this
website.

The assertions are left in deliberately — they document the expected result and they are
what makes the example verifiable.

All examples assume:

```csharp
using Mallard;
```

and the zero-copy and appender examples also need:

```csharp
using Mallard.Types;
```

[src]: https://github.com/SteveKCheng/Mallard/blob/master/Mallard.Tests/DocExamples.cs

## Opening a database and running a query

An empty connection string opens a purely in-memory database; pass a file path to open
or create a database on disk.

[!code-csharp[](../../Mallard.Tests/DocExamples.cs#QuickStart)]

@Mallard.DuckDbConnection.ExecuteNonQuery* runs a statement and discards any result.
@Mallard.DuckDbConnection.ExecuteValue* runs a query and returns the first cell of the
first row, converted to `T`.

## Prepared statements and parameters

Prepare once, execute many times. Parameters can be addressed by name (`$symbol`) or by
one-based position (`$1`), and values are written through strongly-typed setters so that
no boxing occurs.

[!code-csharp[](../../Mallard.Tests/DocExamples.cs#NamedParameters)]

> [!NOTE]
> Parameter indices are **one-based**, matching DuckDB and SQL convention, not
> zero-based like .NET collections.

## Reading results column by column

DuckDB returns results in *chunks* of up to a few thousand rows. Within a chunk, data is
stored by column. @Mallard.DuckDbResult.ProcessAllChunks* hands you a
@Mallard.DuckDbChunkReader for each one.

Calling `GetColumn<T>` checks the column's type once, for the whole chunk. Reading
individual values afterwards does no further type checking.

[!code-csharp[](../../Mallard.Tests/DocExamples.cs#ReadColumns)]

> [!IMPORTANT]
> The @Mallard.DuckDbChunkReader is a `ref struct` that is only valid for the duration of
> the callback — the chunk it reads is released as soon as the callback returns. The
> compiler enforces this: you cannot store it in a field or let it escape. To carry
> results out, close over a local, as above.

## Reading without copying

For unmanaged element types, `GetColumnRaw<T>` plus `AsSpan()` gives a
`ReadOnlySpan<T>` over DuckDB's own buffer for that chunk. Nothing is copied and no GC
object is allocated for the values.

[!code-csharp[](../../Mallard.Tests/DocExamples.cs#ZeroCopyRead)]

This overload of `ProcessAllChunks` takes an `accumulate` function and a `seed`, and
folds the per-chunk results together — the natural shape when each chunk produces a
partial answer.

> [!WARNING]
> The two-argument overload of `ProcessAllChunks` — the one without `accumulate` and
> `seed` — does not currently return the last chunk's value. Use the accumulating
> overload, or close over a local, when you need a result out of the callback.

## Inserting many rows with an appender

An appender is DuckDB's fast bulk-insert path, considerably faster than executing
individual `INSERT` statements. Values are written column by column within a row, then
`FinishRow` advances. Rows are flushed when the appender is disposed.

[!code-csharp[](../../Mallard.Tests/DocExamples.cs#BulkAppend)]

> [!NOTE]
> Appenders currently support simple column types. Nested types such as `LIST` and
> `STRUCT` are not supported yet.

## Adding an example

1. Add a `[Test]` method to `Mallard.Tests/DocExamples.cs`, wrapping the part you want
   published in a `#region`.
2. Reference it from Markdown:

   ```
   [!code-csharp[](../../Mallard.Tests/DocExamples.cs#YourRegion)]
   ```

   or attach it to an API page by adding a `docfx/apidoc/<Uid>.md` overwrite file.
3. Run `python3 docfx/check-snippets.py`.

Step 3 matters. If a region is renamed or removed, DocFX renders an **empty code block**
and still reports `0 warning(s)` — the documentation breaks silently. `check-snippets.py`
catches that, and runs in CI ahead of the docs build.
