# DuckDB.NET

*WORK IN PROGRESS*

.NET bindings for [DuckDB](https://duckdb.org/).

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

There is another, much more mature project, also called [DuckDB.NET](https://duckdb.net/docs/introduction.html).
Needless to say, you should certainly look there if you need DuckDB working now in your .NET code.

I only found out about that other project after starting to write this code on my own.  I might still continue 
this project though for a more personal reason: practicing writing good C# code.  In particular, I like to exploit 
the abilities of recent versions of .NET to write C# that has "close to the metal" performance yet remain (relatively) safe —
similar to Rust, even though C# does not have the same sophistication of analysis in borrowing/aliasing references.

For example, in the unit test you can see how "normal" C# code can consume whole vectors of data from DuckDB
without intermediate copying or heavy conversions involving GC objects.  I think this feature should be
useful in applications involving machine learning or data science.  An ADO.NET-based interface would just 
not be performant enough, and so I do not put high priority on it.

To avoid confusion for everyone, I will be renaming this project soon.
