---
_layout: landing
_disableToc: true
_disableAffix: true
title: Mallard
description: Memory-safe, high-performance .NET bindings for DuckDB.
---

# Mallard

**Alternative .NET bindings for [DuckDB](https://duckdb.org/) — memory-safe, thread-safe,
and built to read whole columns without copying them.**

> [!WARNING]
> Mallard is a work in progress. The library works and is tested, but the API is not
> yet stable and it has not been published to NuGet. See
> [what works today](#what-works-today) before depending on it.

## Install

Requires **.NET 10**. The build downloads the native DuckDB library for your platform
automatically.

```
dotnet add package Mallard
```

## At a glance

[!code-csharp[](../Mallard.Tests/DocExamples.cs#QuickStart)]

Every example in this documentation is a unit test that runs in Mallard's test suite,
extracted at build time. If an example stops compiling or stops passing, the build
breaks. See [Examples](articles/examples.md) for more.

## Why Mallard

DuckDB is a column-oriented database. Most .NET database APIs are row-oriented, and
getting data out of them means boxing every value, allocating a GC object per row, and
re-checking the type of every cell. For the analytic workloads DuckDB is built for,
that overhead dominates.

Mallard's primary API is shaped like the database underneath it.

**Read columns, not rows.** A column's type is checked once per chunk, not once per
value:

[!code-csharp[](../Mallard.Tests/DocExamples.cs#ReadColumns)]

**Read without copying.** For primitive types you can take a `ReadOnlySpan<T>` pointing
straight at DuckDB's own memory — no copy, no boxing, no GC allocation for the values:

[!code-csharp[](../Mallard.Tests/DocExamples.cs#ZeroCopyRead)]

This is the feature that motivates the library. It should be useful for machine learning
and data science workloads, where column-oriented processing is the normal state of
affairs.

**And it stays safe.** Handing out pointers into native memory is normally where a
binding starts documenting which misuses corrupt your heap. Mallard instead uses
`ref struct` to make the compiler enforce that those spans cannot outlive the chunk they
point into. If you do not write `unsafe` code yourself, no misuse of the public API
should be able to crash the runtime.

## Design priorities

In order, and the order matters:

1. **Memory and thread safety in the public API.** Not compromisable. Misuse of the
   public API should raise an exception, never corrupt memory.
2. **Efficiency**, and an API that does not get in the way of it. Avoid unnecessary GC
   objects and virtual calls.
3. An API that leans **functional** rather than "enterprisey" object orientation. Many
   properties are immutable once constructed — for thread safety and for simplicity.
4. **Good coverage in documentation and testing**, including of internals, so you can
   judge the quality of the construction yourself.

Mallard targets current .NET only and makes no concession to .NET Framework 4.x. It uses
`ref struct`, function pointers, and generic specialisation to get close-to-the-metal
performance under the safety constraint above.

## Where to go next

| | |
|---|---|
| [Examples](articles/examples.md) | Worked examples, all of them executable tests |
| [API reference](api/Mallard.yml) | Generated from the XML doc comments |
| [`DuckDbConnection`](api/Mallard.DuckDbConnection.yml) | The type you start from |
| [GitHub](https://github.com/SteveKCheng/Mallard) | Source, issues, build status |

## What works today

- Executing SQL queries and reading results incrementally
- Prepared statements with parameter binding
- Reading values with strong typing and no boxing; zero-copy reads of primitive types
- Writing parameter values without boxing, including from `ReadOnlySpan`
- Type checks done once per column, not per value
- ADO.NET-compatible `IDbConnection`, `IDbCommand` and `IDataReader`
- Nulls checked explicitly, or flagged implicitly via `Nullable<T>` element types
- Appenders for simple types, row-wise and column-wise through `Span<T>`
- NuGet packaging (not yet published)
- Tested on Windows and Linux

**Supported DuckDB types** — all fixed-width integers; floating-point; `VARINT` →
`BigInteger`; `DECIMAL` → `decimal`; enumerations; `VARCHAR` → `string`; `BITSTRING` →
`BitArray`; `BLOB` → `byte[]`; `LIST` → array or `ImmutableArray<T>`; `DATE` →
`DateOnly`/`DateTime`; `TIMESTAMP` → `DateTime`; time intervals; `UUID` → `Guid`.

## Not there yet

- DuckDB types: `TIME`, `STRUCT`, fixed-length arrays, timestamps in units other than
  microseconds
- Not every readable type can yet be bound as a prepared-statement parameter
- Querying extended type information (for `STRUCT`, `ENUM`, …)
- User-defined functions
- Appenders on nested columns
- Adapters for `Microsoft.Data.Analysis.DataFrame`
- Full AOT compatibility — reflection is still required for composite types such as
  `MyEnum[]`

## Relation to DuckDB.NET

[DuckDB.NET](https://duckdb.net/docs/introduction.html) is a much more mature project,
and if you need DuckDB working in .NET today you should look there first.

Mallard's author found out about it only after starting this code, and continues for a
more personal reason: practising good, solid C#. Specifically, exploiting recent .NET
features to write code with close-to-the-metal performance that nonetheless stays
safe — in spirit similar to Rust, even though C# lacks the same sophistication in
borrow and alias analysis. That difference in emphasis is visible in the safety
guarantee: DuckDB.NET's documentation notes in several places that misuse of its API can
cause memory corruption. For Mallard that is not an acceptable outcome.

## About the name

*Mallard* is a species of wild duck — the *wild* moniker seemed appropriate. The English
word is cognate, via Latin, with the
[French *malard*](https://www.dictionnaire-academie.fr/article/A9M0304), which keeps the
original sense of "male duck".
