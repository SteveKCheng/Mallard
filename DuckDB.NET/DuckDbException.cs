using DuckDB.C_API;
using System;

namespace DuckDB;

public class DuckDbException : Exception
{
    public DuckDbException(string? message) : base(message)
    {
    }
    internal static void ThrowOnFailure(duckdb_state status, string errorMessage)
    {
        if (status != duckdb_state.DuckDBSuccess)
            throw new DuckDbException(errorMessage);
    }
}
