# Mallard

![logo](art/mallard.png)

*WORK IN PROGRESS*

Alternative .NET bindings for [DuckDB](https://duckdb.org/).

Do not expect anything to work right now.  This page will be updated as the code approaches a working state.

## Build status

[![Build&Test status](https://github.com/SteveKCheng/Mallard/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/SteveKCheng/Mallard/actions/workflows/build-and-test.yml)

## Requirements

  - .NET 9 or later

Mallard requires .NET 9+ for substituting ``ref struct`` types in generic parameters.
.NET 10 (version with long-term support) is currently in preview, and Mallard will switch
to target that exclusively once a release version is widely available.

## Build instructions

  - Simply open the solution file ``Mallard.slnx`` in your .NET IDE and tell it to build;
  - or, on the command line, run ``dotnet build`` from the top-level directory.

## Getting DuckDB

The build process will automatically download the native library files for DuckDB 
(e.g. ``duckdb.dll`` for Windows), assuming the platform you are running
.NET on is supported.  If you prefer, you can download the files yourself:

  - Go to [the DuckDB releases page](https://github.com/duckdb/duckdb/releases) and get the ``libduckdb-*.zip`` file
    appropriate for your platform.
  - Extract ``duckdb.dll`` (or ``duckdb.so`` etc.) and put it inside the directory ``native/‹version›/‹platform›/``.
    - ``‹platform›`` is the .NET run-time identifier, e.g. ``win-x64`` for Windows on x64, ``linux-x64`` for 
    common glibc-based Linux distributions on x64.
    - ``‹version›`` is the DuckDB version that is set in ``Mallard.Runtime/DuckDbVersion.props``.  If you want to change the version you can do so in that file or in ``Directory.Build.props``.
  - If you want to use another/custom build of DuckDB, you can place the library files in the same locations indicated.  You can run Mallard & DuckDB on platforms that are not officially supported with a binary release this way.

### To run tests

  - Use your IDE's “Test Explorer” on the project ``Mallard.Tests``.
    - On JetBrains rider, you have to select “Enable Testing Platform support” under the IDE's settings.  
    - On Visual Studio Code: “Use Testing Platform Protocol” under C# Dev Kit's settings must be enabled.
    - The latest versions of Visual Studio should not require further configuration.
    - See [TUnit: Getting Started » Running your tests](https://tunit.dev/docs/getting-started/running-your-tests) for more details.
  - Or, execute the command: ``dotnet run`` from the ``Mallard.Tests/`` directory.

The test programs support AOT (ahead-of-time) compilation.  To run in AOT mode:

  - Run ``dotnet publish`` in the ``Mallard.Tests/`` directory.
  - Then run the executable ``out/publish/Mallard.Tests/release/Mallard.Tests``.

### To create NuGet packages

  - For the .NET bindings: 
      - Execute ``dotnet pack`` from the ``Mallard/`` directory.
      - Output will be in ``out/package/release/Mallard.«version».nupkg``.
      - Note that the ``«version»`` here is that of Mallard, not of DuckDB.
  - For the DuckDB native library (re-packaged into NuGet packages):
      - Execute ``dotnet pack -p:RuntimeIdentifier=«platform»`` from the ``Mallard.Runtime/`` directory.
      - Output will be in ``out/package/release/Mallard.Runtime.«platform».«version».nupkg``.
      - ``«version»`` here refers to DuckDB's version.

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

## What works today (as of September 15, 2025)

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
    - [X] date (DATE → ``System.DateOnly`` or ``System.DateTime``)
    - [X] timestamp (TIMESTAMP → ``System.DateTime``)
    - [X] time intervals
    - [X] UUIDs (UUID → ``System.Guid``)
  - [X] ADO.NET-compatible interfaces
    - [X] ``System.Data.IDbConnection``
    - [X] ``System.Data.IDbCommand``
    - [X] ``System.Data.IDataReader``
  - [X] Null values in database can be checked explicitly or flagged implicitly with ``System.Nullable<T>`` element types
  - [X] Thread-safe and memory-safe public API 
    - If you do not use unsafe code, then even improper use of the public API should not crash the .NET run-time
    - ``ref struct`` is used to carefully control lifetime so user sees no dangling pointers to DuckDB native objects
  - [X] Design minimizes unnecessary GC allocations
  - [X] Tested on Windows and Linux.  (The .NET library makes no Windows- or Linux-specific assumptions and should be portable to all the desktop platforms supported by .NET.)
  - [X] NuGet packaging (not published yet, however)

## Major features missing

  - DuckDB types
    - TIME
    - timestamps in units other than microseconds
    - STRUCT
    - arrays (fixed-length)
  - Not all types whose values can be read (from DuckDB vectors) can be bound to parameters in prepared statements
  - Caching of objects
    - Open DuckDB database objects
  - User-defined functions
  - Appenders (DuckDB's API to insert many values quickly into a table)
  - Adapters for ``Microsoft.Data.Analysis.DataFrame``
  - Error reporting (exceptions) needs to be regularized
  - Not all features that work have been thoroughly tested
  - Not completely compatible with AOT compilation
    - Reflection is required at least for conversion of composite types, e.g. ``MyEnum[]``
    - Does not use MSIL code generation but does instantiate generic methods for types only known at run-time
    - Conversion for primitive types does not require reflection; all code is statically visible to the compiler

## Design philosophy

As you might have guessed, Mallard's author is opinionated on the design.  The priorities, in order, are:

  1. Always ensure memory and thread safety in the public API.
  2. Highly efficient implementation, and an API that does not impair that efficiency.  
     Avoid unnecessary use of GC objects and virtual method calls.
  3. API shape leans towards being “functional”, and not “enterprisey” object orientation. 
  4. Good coverage in documentation and testing.
  
In detail:
  
  - [1] is not compromisable.  (In contrast, DuckDB.NET states, in several places in its documentation, that misuse of its API can
    cause memory corruption.  That is not acceptable for Mallard, which aims to be a well-behaved member of the .NET ecosystem.)
  - Mallard uses the latest .NET and C# features to achieve the best efficiency [2] under the constraint of [1].  The notable
    of those features are: 
      - ``ref struct``
      - function pointers (internally, not exposed in the public API
      - specialization of generics (taking advantage of the fact that generics on value types are always monomorphized)
  - Mallard is not AOT-compatible currently but there are plans to make it so.
  - Mallard does not bother to be compatible with legacy .NET platforms such as .NET Framework 4.x.  If you want
    efficiency, you probably would have upgraded already.
  - On [3], Mallard prioritizes its own high-performance APIs, but will also implement the standard ADO.NET API 
    in the most efficient yet safe way possible.  
  - For both reasons of thread safety, and simplicity of its own implementation, Mallard's APIs make certain 
    attributes of objects/values be immutable once constructed. Mallard's author in particular does not hold in 
    high regard ADO.NET's tendency (and more generally, of the “enterprise” code style popular in the early 2000s)
    to wrap everything in layers of objects with read/write properties.
  - The “test bed” for Mallard's API will be “data science” applications, where column-oriented processing
    of data is a normal state of affairs, which of course plays to DuckDB's strengths. (Traditional database APIs
    like ADO.NET are usually row-oriented.)
  - On [4], the author even takes the time to document internal aspects of the implementation.  This may help
    you evaluate the quality of the construction of this library. 
  - The testing of this library is unfortunately, at the moment, fairly weak but this should hopefully improve
    in the future.

## About the name

  - *Mallard* is a species of wild duck.  I think the *wild* moniker is quite appropriate.
  - This English word is cognate (via Latin) to the [French word *malard*](https://www.dictionnaire-academie.fr/article/A9M0304), 
    which retains the original sense of "male duck".  (I originally thought to use the French word,
    just to be a little unique, but ultimately decided not, to avoid any unintended sexist connotations.)
  - I had also considered the Japanese word for mallard which is *magamo* マガモ【真鴨】. Literally, it 
    means "true duck" — which sounds cool, though I checked the dictionary and it says the 
    [*ma-* 【真】 prefix](https://kotobank.jp/word/%E7%9C%9F-4672#w-632658)
    when applied to animals, simply refers to the animal being a representative species.

