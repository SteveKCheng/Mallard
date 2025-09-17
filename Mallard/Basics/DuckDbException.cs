using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

public class DuckDbException : Exception
{
    /// <summary>
    /// The category of error (error code) reported by DuckDB.
    /// </summary>
    public DuckDbErrorKind ErrorKind { get; private set; }
    
    public DuckDbException(string? message) : base(message)
    {
    }
    
    internal static void ThrowOnFailure(duckdb_state status, string errorMessage)
    {
        if (status != duckdb_state.DuckDBSuccess)
            throw new DuckDbException(errorMessage);
    }

    [DoesNotReturn]
    internal static void ThrowForResultFailure(ref duckdb_result nativeResult)
    {
        var errorMessage = NativeMethods.duckdb_result_error(ref nativeResult);
        var errorKind = NativeMethods.duckdb_result_error_type(ref nativeResult);
        throw new DuckDbException(errorMessage) { ErrorKind = errorKind };
    }
}
