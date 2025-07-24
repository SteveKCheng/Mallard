# Mallard

*WORK IN PROGRESS*

Alternative .NET bindings for [DuckDB](https://duckdb.org/).

Do not expect anything to work right now.  This page will be updated as the code approaches a working state.

## Requirements

  - .NET 9 or later

## Build instructions

You need to separately download the native library files for DuckDB, e.g. ``duckdb.dll`` for Windows. 

  - Go to [the DuckDB releases page](https://github.com/duckdb/duckdb/releases) and get the ``libduckdb-*.zip`` file
    appropriate for your platform.
  - Extract ``duckdb.dll`` (or ``duckdb.so`` etc.) and put it inside the directory ``native/‹platform›/``.
  - ``‹platform›`` is the .NET run-time identifier, e.g. ``win-x64`` for Windows on x64, ``linux-x64`` for 
    common glibc-based Linux distributions on x64.

## Relation to other .NET bindings

There is another, much more mature project, called [DuckDB.NET](https://duckdb.net/docs/introduction.html).
Needless to say, you should certainly look there if you need DuckDB working now in your .NET code.

I only found out about that other project after starting to write this code on my own.  I might still continue 
this project though for a more personal reason: practicing writing good C# code.  In particular, I like to exploit 
the abilities of recent versions of .NET to write C# that has "close to the metal" performance yet remain (relatively) safe —
similar to Rust, even though C# does not have the same sophistication of analysis in borrowing/aliasing references.

For example, in the unit test you can see how "normal" C# code can consume whole vectors of data from DuckDB
without intermediate copying or heavy conversions involving GC objects.  I think this feature should be
useful in applications involving machine learning or data science.  An ADO.NET-based interface would just 
not be performant enough, and so I do not put high priority on it.

## What works today (as of July 23, 2025)

  - [X] Executing SQL queries and reading results incrementally
  - [X] Prepared statements with parameter binding
  - [X] Reading values from DuckDB columns with strong typing and no boxing (unless explicitly requested)
  - [X] Type checks are done once per column not for each value read (high performance)
  - DuckDB types supported
    - [X] all fixed-width integers
    - [X] floating-point numbers
    - [X] variable-length integers (VARINT → ``System.Numerics.BigInteger``)
    - [X] decimals (DECIMAL → ``System.Decimal``)
    - [X] enumerations 
    - [X] strings (VARCHAR → ``System.String``)
    - [X] bit strings (BITSTRING → ``System.Collections.BitArray``)
    - [X] blobs (BLOB → ``byte[]``)
    - [X] variable-length lists (LIST → .NET array or ``System.Collections.ImmutableArray<T>``)
    - [X] date (DATE → ``System.Date``)
    - [X] timestamp (TIMESTAMP → ``System.DateTime``)
    - [X] time intervals
  - [X] Null values in database can be checked explicitly or flagged implicitly with ``System.Nullable<T>`` element types
  - [X] Thread-safe and memory-safe public API 
    - If you do not use unsafe code, then even improper use of the public API should not crash the .NET run-time
    - ``ref struct`` is used to carefully control lifetime so user sees no dangling pointers to DuckDB native objects
  - [X] Design minimizes unnecessary GC allocations

## Major features missing

  - DuckDB types
    - TIME
    - timestamps in units other than microseconds
    - STRUCT
    - arrays (fixed-length)
  - Not all types whose values can be read (from DuckDB vectors) can be bound to parameters in prepared statements
  - ``System.Data`` (ADO.NET) -compatible interfaces
  - Caching of objects
    - Open DuckDB database objects
    - Certain converters (e.g. for enums) that are heavy to construct
  - User-defined functions
  - Appenders (DuckDB's API to insert many values quickly into a table)
  - Proper NuGet packaging
  - Adapters for ``Microsoft.Data.Analysis.DataFrame``
  - Error reporting (exceptions) needs to be regularized
  - Not all features that work have been thoroughly tested
  - Not completely compatible with AOT compilation
    - Reflection is required at least for conversion of composite types, e.g. ``MyEnum[]``
    - Does not use MSIL code generation but does instantiate generic methods for types only known at run-time
    - Conversion for primitive types does not require reflection; all code is statically visible to the compiler
  - Only tested on Windows so far
    - Although the .NET library makes no Windows-specific assumptions and should be portable to all the desktop platforms supported by .NET

## About the name

  - *Mallard* is a species of wild duck.  I think the *wild* moniker is quite appropriate.
  - This English word is cognate (via Latin) to the [French word *malard*](https://www.dictionnaire-academie.fr/article/A9M0304), 
    which retains the original sense of "male duck".  (I originally thought to use the French word,
    just to be a little unique, but ultimately decided not, to avoid any unintended sexist connotations.)
  - I had also considered the Japanese word for mallard which is *magamo* マガモ【真鴨】. Literally, it 
    means "true duck" — which sounds cool, though I checked the dictionary and it says the 
    [*ma-* 【真】 prefix](https://kotobank.jp/word/%E7%9C%9F-4672#w-632658)
    when applied to animals, simply refers to the animal being a representative species.

