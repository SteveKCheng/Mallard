using Mallard.Interop;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// Represents a database-level error from DuckDB.
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

    internal DuckDbException(string? message, DuckDbErrorKind errorKind) : base(message)
    {
        ErrorKind = errorKind;
    }
    
    internal static void ThrowOnFailure(duckdb_state status, string errorMessage)
    {
        if (status != duckdb_state.DuckDBSuccess)
            throw new DuckDbException(errorMessage);
    }

    internal static unsafe void ThrowOnFailure(duckdb_state status, _duckdb_error_data* errorData, string defaultErrorMessage = "An error occurred in DuckDB. ")
    {
        if (status != duckdb_state.DuckDBSuccess)
            ThrowForErrorData(errorData, defaultErrorMessage);
        else if (errorData != null)
            NativeMethods.duckdb_destroy_error_data(ref errorData);
    }

    [DoesNotReturn]
    internal static void ThrowForResultFailure(ref duckdb_result nativeResult)
    {
        var errorMessage = NativeMethods.duckdb_result_error(ref nativeResult);
        var errorKind = NativeMethods.duckdb_result_error_type(ref nativeResult);
        throw new DuckDbException(errorMessage) { ErrorKind = errorKind };
    }

    [DoesNotReturn]
    internal static unsafe void ThrowForErrorData(_duckdb_error_data* errorData, string defaultErrorMessage = "An error occurred in DuckDB. ")
    {
        DuckDbException exceptionToThrow;
        try
        {
            if (errorData != null && NativeMethods.duckdb_error_data_has_error(errorData))
            {
                var errorMessage = NativeMethods.duckdb_error_data_message(errorData);
                var errorKind = NativeMethods.duckdb_error_data_error_type(errorData);
                exceptionToThrow = new DuckDbException(string.IsNullOrEmpty(errorMessage) ? defaultErrorMessage : errorMessage)
                {
                    ErrorKind = errorKind
                };
            }
            else
            {
                exceptionToThrow = new DuckDbException(defaultErrorMessage);
            }
        }
        finally
        {
            if (errorData != null)
                NativeMethods.duckdb_destroy_error_data(ref errorData);
        }

        throw exceptionToThrow;
    }

    [DoesNotReturn]
    internal static unsafe void ThrowForAppenderFailure(_duckdb_appender* nativeAppender, string defaultErrorMessage = "Failed to append value. ")
    {
        if (nativeAppender != null)
        {
            var errorData = NativeMethods.duckdb_appender_error_data(nativeAppender);
            ThrowForErrorData(errorData, defaultErrorMessage);
        }

        throw new DuckDbException(defaultErrorMessage);
    }
}
