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

## About the name

  - *Mallard* is a species of wild duck.  I think the *wild* moniker is quite appropriate.
  - This English word is cognate (via Latin) to the [French word *malard*](https://www.dictionnaire-academie.fr/article/A9M0304), 
    which retains the original sense of "male duck".  (I originally thought to use the French word,
    just to be a little unique, but ultimately decided not, to avoid any unintended sexist connotations.)
  - I had also considered the Japanese word for mallard which is *magamo* マガモ【真鴨】. Literally, it 
    means "true duck" — which sounds cool, though I checked the dictionary and it says the 
    [*ma-* 【真】 prefix](https://kotobank.jp/word/%E7%9C%9F-4672#w-632658)
    when applied to animals, simply refers to the animal being a representative species.

