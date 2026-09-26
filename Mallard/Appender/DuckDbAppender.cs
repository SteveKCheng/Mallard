using System;
using Mallard.Interop;

namespace Mallard;

/// <summary>
/// Efficiently inserts data rows, in bulk, to a DuckDB table.
/// </summary>
/// <remarks>
/// <para>
/// This class exposes the "appender" functionality from DuckDB C's API.
/// When inserting many data rows, DuckDB's appenders can do so faster, versus using a prepared statement
/// to insert a row and executing it multiple times.  
/// </para>
/// <para>
/// After obtaining this object from <see cref="DuckDbConnection.CreateAppender(string?,string?,string)" />
/// or one of its convenience overloads, add data values for a row by calling <see cref="Append" />
/// and then following up with a call to a method of <see cref="DuckDbValue" />.  Finish up one row's data
/// by calling <see cref="FinishRow" />.  Repeat for each row.  Then dispose of this object.
/// </para>
/// <para>
/// <a href="https://duckdb.org/docs/current/clients/c/appender">DuckDB Documentation: Client APIs : C: Appender</a>
/// </para>
/// </remarks>
public sealed unsafe partial class DuckDbAppender : IDisposable
{
    #region Resource management
    
    /// <summary>
    /// The native DuckDB "appender" object.  Is non-null until disposed.
    /// </summary>
    private _duckdb_appender* _nativeObj;
    
    /// <summary>
    /// Ensures that only one thread can manipulate the native "appender" object at the same time.
    /// </summary>
    private Barricade _barricade;
    
    /// <summary>
    /// Increments every time a value is appended.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This counter exists to guard against misuse of the C# API.  When an instance of <see cref="Slot" />
    /// receives the value to append and successfully passes it to DuckDB, the counter is incremented so
    /// the same instance of <see cref="Slot" /> cannot be used to set another value.
    /// </para>
    /// </remarks>
    private ulong _sequenceCounter;
    
    internal DuckDbAppender(_duckdb_connection* nativeConn,
                            string? catalogName,
                            string? schemaName,
                            string tableName)
    {
        var status = NativeMethods.duckdb_appender_create_ext(nativeConn, 
                                                     catalogName, 
                                                     schemaName, 
                                                     tableName, 
                                                     out var nativeObj);
        try
        {
            if (status == duckdb_state.DuckDBError)
            {
                throw new DuckDbException("Failed to create appender. ");
            }
        }
        catch
        {
            NativeMethods.duckdb_appender_destroy(ref nativeObj);
            throw;
        }

        _nativeObj = nativeObj;
    }

    private void DisposeImpl(bool disposing)
    {
        if (!_barricade.PrepareToDisposeOwner())
            return;

        NativeMethods.duckdb_appender_destroy(ref _nativeObj);
    }

    /// <summary>
    /// Destructor which will dispose this object if it has yet been already.
    /// </summary>
    ~DuckDbAppender()
    {
        DisposeImpl(disposing: false);
    }

    /// <summary>
    /// Disposes this object along with resources allocated in the native DuckDB library for it. 
    /// </summary>
    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }
    
    #endregion

    /// <summary>
    /// Prepare to append one data value (one column's data for the current row).
    /// </summary>
    /// <returns>
    /// An instance of <see cref="Slot" /> that will receive the data value to be appended.
    /// The actual data value to append is supplied by invoking one of the extension methods
    /// from <see cref="DuckDbValue" /> on the new instance. 
    /// </returns>
    /// <remarks>
    /// <para>
    /// In the current implementation, for efficiency,
    /// the <see cref="Slot" /> instance is created without any validity checks, even though in theory
    /// the implementation should check for this method (<see cref="Append" />) being called 
    /// in succession without intervening methods to set the actual data value.  Clients should not
    /// rely on this behavior.
    /// </para>
    /// <para>
    /// Alternatively this class could be designed to implement <see cref="ISettableDuckDbValue" />
    /// directly, without going through a "slot" object.  However, unlike other implementations of
    /// <see cref="ISettableDuckDbValue" />, the "Set" methods have to invoke the underlying "append"
    /// function in DuckDB immediately, so that multiple calls do not set values into the same slot
    /// (i.e. "Set" is not idempotent).  That API shape might be too surprising given how the methods
    /// are named; insisting on a "dummy" "append" call first makes the intent more clear. 
    /// </para>
    /// </remarks>
    public Slot Append() => new Slot(this);

    private void ThrowOnAppendFailure(duckdb_state status)
    {
        if (status != duckdb_state.DuckDBSuccess)
            throw new DuckDbException("Failed to append value. ");
    }

    /// <summary>
    /// Mark that the current row's data values (one for each column) have all been appended. 
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    public void FinishRow()
    {
        using var _ = _barricade.EnterScope(this);
        NativeMethods.duckdb_appender_end_row(_nativeObj);
    }
}
