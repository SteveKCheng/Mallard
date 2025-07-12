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
