using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// Reprsents a database-level error from DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// This type of exception is thrown from Mallard for run-time errors reported by the native DuckDB library.
/// These errors can include failure to open or operate on a database (file),
/// syntax problems in SQL statements and constraint violations.
/// </para>
/// <para>
/// Note that run-time errors originating from misuse of or incorrect arguments to Mallard's API,
/// are generally reported by familiar .NET exceptions such as <see cref="ArgumentException" /> or
/// <see cref="InvalidOperationException" />.  Such errors get detected by .NET code in Mallard
/// before the relevant requests even reach the DuckDB native library.  
/// </para>
/// </remarks>
public sealed class DuckDbException : Exception
{
    /// <summary>
    /// The category of error (error code) reported by DuckDB.
    /// </summary>
    public DuckDbErrorKind ErrorKind { get; private set; }
    
    internal DuckDbException(string? message) : base(message)
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
