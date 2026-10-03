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
/// After obtaining this object from <see cref="DuckDbConnection.CreateTableAppender" />
/// or one of its convenience overloads, add data values for a row by calling <see cref="Append" />
/// and then following up with a call to a method of <see cref="DuckDbValue" />.  Finish up one row's data
/// by calling <see cref="FinishRow" />.  Repeat for each row.  Then dispose of this object.
/// </para>
/// <para>
/// If any method from an appender (instance of this class) throws an error, that appender object
/// is spoiled, and it may not be used in further operations other than disposal.
/// This behavior matches the DuckDB API.  In .NET, <see cref="InvalidOperationException" /> on attempts
/// to use an appender that has already failed once.
/// </para>
/// <para>
/// Appenders participate in any current transactions on the owning <see cref="DuckDbConnection" />.
/// However, beware that on attempting to insert bad data (e.g. has constraint violations), the containing
/// transaction may also be spoiled and must be rolled back.  (This behavior is no different than running
/// a failed <c>INSERT</c> statement in DuckDB SQL within a transaction.)
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
    /// <remarks>
    /// This also guards <see cref="_hasFailed" />: that flag is only ever read or written while
    /// the current thread holds this barricade (via <see cref="Barricade.EnterScope" />), or while
    /// disposing (where <see cref="Barricade.PrepareToDisposeOwner" /> has already claimed exclusive
    /// access), so plain field reads/writes without further synchronization are safe.
    /// </remarks>
    private Barricade _barricade;

    /// <summary>
    /// Set to true once any operation on this appender has reported an error from DuckDB
    /// (and thrown <see cref="DuckDbException" /> for it).
    /// </summary>
    /// <remarks>
    /// <para>
    /// DuckDB's appender does not keep the reported error stable once it has occurred: a further
    /// attempt to append data (or to end a row) after an error may spuriously succeed, or may fail
    /// with unrelated diagnostic information that masks the original problem.  (This was confirmed
    /// by inspecting DuckDB's own implementation of the C API for the appender.)  So, rather than
    /// relying on DuckDB to keep reporting the same error, Mallard tracks the failure itself, and
    /// refuses to perform further operations once this flag is set: see <see cref="CheckNotFailed" />.
    /// </para>
    /// <para>
    /// This flag also changes how <see cref="Dispose" /> behaves: once set, disposal must not attempt
    /// to flush data or report further errors, since doing so risks masking whatever exception was
    /// already thrown to the caller for the original failure.
    /// </para>
    /// </remarks>
    private bool _hasFailed;

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

    /// <summary>
    /// The (old) value of <see cref="_sequenceCounter" /> right after the last flush, chunk append,
    /// or zero initially.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This counter indicates to <see cref="AppendChunk" /> when row-wise activity has occurred 
    /// since the last flush or chunk boundary.  (See development notes in the source tree
    /// for details and rationale.)
    /// </para>
    /// </remarks>
    private ulong _sequenceCounterStart;

    internal DuckDbAppender(_duckdb_connection* nativeConn,
                            string? catalogName,
                            string? schemaName,
                            string tableName)
    {
        var status = NativeMethods.duckdb_appender_create_ext(nativeConn, 
                                                     catalogName, 
                                                     schemaName, 
                                                     tableName, 
                                                     out _nativeObj);
        try
        {
            ThrowOnAppenderFailure(status, "Failed to create appender. ");
        }
        catch
        {
            NativeMethods.duckdb_appender_destroy(ref _nativeObj);
            throw;
        }
    }

    /// <summary>
    /// Creates a query-based appender, backing <see cref="DuckDbConnection.CreateQueryAppender" />.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Rows appended through this object are streamed into a virtual relation (named
    /// <c>appended_data</c>, with columns <c>col1</c>, <c>col2</c>, ... in this first implementation)
    /// that the supplied <paramref name="query" /> reads from, so statements such as
    /// <c>INSERT ... ON CONFLICT</c>, <c>MERGE INTO</c>, or <c>INSERT ... SELECT ... WHERE</c>
    /// can be used, unlike the plain table appender.
    /// </para>
    /// <para>
    /// The column types of the virtual relation cannot be inferred from the query, so they are
    /// supplied explicitly by the caller and mapped to native logical types via
    /// <see cref="DuckDbComplexTypeInfo.MapToNativeLogicalType" />.  DuckDB copies these logical
    /// types, so they are destroyed again before this constructor returns.
    /// </para>
    /// </remarks>
    internal DuckDbAppender(_duckdb_connection* nativeConn,
                            string query,
                            ReadOnlySpan<Type> columnTypes,
                            string? tableName,
                            ReadOnlySpan<string> columnNames)
    {
        var columnCount = columnTypes.Length;

        if (columnNames.Length != 0 && columnNames.Length != columnCount)
            throw new ArgumentException("The names of the data columns must be all specified, or none are. ", nameof(columnNames)); 

        // Native logical-type handles for the virtual relation's columns.  DuckDB copies these
        // during creation, so we own them and destroy them again in the finally block below.
        var nativeTypes = stackalloc _duckdb_logical_type*[columnCount];

        try
        {
            for (var i = 0; i < columnCount; ++i)
                nativeTypes[i] = DuckDbComplexTypeInfo.MapToNativeLogicalType(columnTypes[i]).NativeHandle;

            // Convert columnNames to array of UTF-8 strings.
            using var namesConverter = new Utf8StringConverterState();
            byte** nativeNames = null;
            if (columnNames.Length != 0)
            {
                int namesBufferSize = Utf8StringConverterState.SuggestedBufferSize;
                var namesBuffer = stackalloc byte[namesBufferSize];
                var nativeNamesArray = stackalloc byte*[columnNames.Length];
                namesConverter.ConvertStringArrayToUtf8(columnNames, nativeNamesArray, namesBuffer, namesBufferSize);    
                nativeNames = nativeNamesArray;
            }
            
            var status = NativeMethods.duckdb_appender_create_query(nativeConn,
                                                                    query,
                                                                    columnCount,
                                                                    nativeTypes,
                                                                    table_name: tableName,
                                                                    column_names: nativeNames,
                                                                    out _nativeObj);
            try
            {
                ThrowOnAppenderFailure(status, "Failed to create query-based appender. ");
            }
            catch
            {
                NativeMethods.duckdb_appender_destroy(ref _nativeObj);
                throw;
            }
        }
        finally
        {
            for (var i = 0; i < columnCount; ++i)
            {
                if (nativeTypes[i] != null)
                    NativeMethods.duckdb_destroy_logical_type(ref nativeTypes[i]);
            }
        }
    }

    /// <remarks>
    /// <para>
    /// When <paramref name="disposing" /> is false, this method is running on the finalizer thread,
    /// so it must never throw an exception (an unhandled exception from a finalizer terminates the
    /// process), and must not do anything beyond releasing the native resources.
    /// </para>
    /// <para>
    /// Otherwise, if <see cref="_hasFailed" /> has already been set, some earlier operation has
    /// already reported (and thrown) a DuckDB error for this appender, so this method just destroys
    /// the native object without attempting to flush or report anything further, to avoid masking
    /// that earlier exception.
    /// </para>
    /// <para>
    /// Otherwise, this is the first attempt to close the appender: <c>duckdb_appender_close</c> is
    /// called first (which flushes buffered data) so that, if it fails, <c>duckdb_appender_error_data</c>
    /// can still retrieve diagnostics before <c>duckdb_appender_destroy</c> frees the underlying memory.
    /// </para>
    /// </remarks>
    private void DisposeImpl(bool disposing)
    {
        if (!_barricade.PrepareToDisposeOwner())
            return;

        try
        {
            if (disposing && !_hasFailed)
            {
                var status = NativeMethods.duckdb_appender_close(_nativeObj);
                if (status != duckdb_state.DuckDBSuccess)
                {
                    var errorData = NativeMethods.duckdb_appender_error_data(_nativeObj);
                    DuckDbException.ThrowForErrorData(ref errorData, "Failed to close appender. ");
                }
            }
        }
        finally
        {
            NativeMethods.duckdb_appender_destroy(ref _nativeObj);

            // The reusable data chunk for bulk chunk-writing (if one was ever created) is
            // owned by this appender and must be freed regardless of how disposal proceeded.
            if (_writeChunk != null)
                NativeMethods.duckdb_destroy_data_chunk(ref _writeChunk);
        }
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
    /// <exception cref="DuckDbException">
    /// Flushing the data that has been appended so far failed, e.g. due to a constraint violation,
    /// and this is the first time this appender has reported an error.  If this appender has already
    /// reported an error from an earlier operation (e.g. from <see cref="FinishRow" /> or from setting
    /// a value), this method does not throw, to avoid masking that earlier exception; call
    /// <see cref="Close" /> explicitly beforehand if that error needs to be observed at a well-defined
    /// point instead of arbitrarily, e.g. from a <c>using</c> statement.
    /// </exception>
    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Explicitly close this appender, equivalent to calling <see cref="Dispose" />.
    /// </summary>
    /// <remarks>
    /// This method exists so that client code that wants to handle a possible error while flushing
    /// the appended data can do so at an obvious point in the code, rather than being surprised by
    /// an exception thrown implicitly when a <c>using</c> statement exits and calls <see cref="Dispose" />.
    /// Otherwise, there is no difference between calling this method and calling <see cref="Dispose" />;
    /// in particular, this method also releases the native resources held by this appender, so this
    /// object must not be used afterward (except that calling <see cref="Dispose" /> again is harmless).
    /// </remarks>
    /// <exception cref="DuckDbException">
    /// Flushing the data that has been appended so far failed, e.g. due to a constraint violation,
    /// and this is the first time this appender has reported an error.
    /// </exception>
    public void Close() => Dispose();

    /// <summary>
    /// Throw <see cref="InvalidOperationException" /> if this appender has already reported an error
    /// from DuckDB, since continuing to append rows to it afterward is a logic error on the caller's part.
    /// </summary>
    /// <remarks>
    /// Must only be called while the current thread holds <see cref="_barricade" />.
    /// </remarks>
    private void CheckNotFailed()
    {
        if (_hasFailed)
        {
            throw new InvalidOperationException(
                "This DuckDbAppender has already reported an error from DuckDB and can no longer be " +
                "used to append data.  Dispose of it (or call Close) and create a new appender instead. ");
        }
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

    /// <remarks>
    /// Must only be called while the current thread holds <see cref="_barricade" />, or from the constructor, 
    /// since on failure this sets <see cref="_hasFailed" /> without further synchronization.
    /// </remarks>
    private void ThrowOnAppenderFailure(duckdb_state status, string defaultErrorMessage = "Failed to append value. ")
    {
        if (status == duckdb_state.DuckDBSuccess)
            return;

        _hasFailed = true;

        var errorData = NativeMethods.duckdb_appender_error_data(_nativeObj);
        DuckDbException.ThrowForErrorData(ref errorData, defaultErrorMessage);
    }

    /// <summary>
    /// Mark that the current row's data values (one for each column) have all been appended. 
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// This appender has already reported an error from DuckDB and can no longer be used.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// Failed to finish the row, e.g. because fewer columns were appended than expected.
    /// </exception>
    public void FinishRow()
    {
        using var _ = _barricade.EnterScope(this);
        CheckNotFailed();
        _sequenceCounter++; // Invalidate any slots out there
        var status = NativeMethods.duckdb_appender_end_row(_nativeObj);
        ThrowOnAppenderFailure(status, "Failed to finish row. ");
    }

    /// <summary>
    /// Materializes ("flushes") the appended rows and chunks that have been internally buffered by DuckDB,
    /// into the destination table (or execute the SQL statement that is to receive the rows).  
    /// </summary>
    /// <remarks>
    /// <para>
    /// DuckDB buffers data before they are materialized ("flushed") into database storage.  Row-level appends
    /// (via <see cref="Append" />) are stored in a hidden chunk that DuckDB manages.
    /// Appends of whole chunks (via <see cref="AppendChunk{TState}" />) also go into a collection
    /// of pending chunks.
    /// </para>
    /// <para>
    /// This method tells DuckDB to materialize those pending chunks into storage. (If a SQL transaction is active,
    /// the data in the chunks will be inserted as part of the transaction —
    /// subject to roll-back as with any other SQL statement that mutates the database.)
    /// </para>
    /// <para>
    /// DuckDB does not buffer an unlimited number of chunks, but will implicitly materialize them
    /// after some number of chunks; the default is 100.  The user may call this method to
    /// force materialization earlier.  Any constraint violations in the data will be detected at that point.
    /// </para>
    /// <para>
    /// Data is implicitly flushed also when the appender is closed (via <see cref="Dispose" /> or
    /// <see cref="Close" />), so this method need not be called unless to check-point progress 
    /// while appending a large amount of data. 
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// This appender has already reported an error from DuckDB and can no longer be used.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// DuckDB encountered an error while attempting to flush the data, such as a constraint violation.
    /// Attempting to flush while the current row has only been partially written will also throw this exception.
    /// </exception>
    public void Flush()
    {
        using var _ = _barricade.EnterScope(this);
        CheckNotFailed();
        
        // Invalidate any slots out there and reset counter for flush
        _sequenceCounterStart = ++_sequenceCounter; 

        FlushInternal();
    }

    /// <summary>
    /// Common code for flushing data to DuckDB, called by <see cref="Flush" /> and <see cref="AppendChunk" />;
    /// the caller must take lock on this object and check/set sequence counters.
    /// </summary>
    private void FlushInternal()
    {
        var status = NativeMethods.duckdb_appender_flush(_nativeObj);
        ThrowOnAppenderFailure(status, "Failed to flush pending data in appender. ");
    }

    /// <summary>
    /// The size of the chunks allocated by DuckDB (via <see cref="AppendChunk{TState}" />).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This size is actually a compile-time constant in DuckDB, but the actual value is not
    /// part of DuckDB's public contract.  Knowing this value makes it easier to write code
    /// to batch writing of an arbitrary number of rows into chunks. 
    /// </para>
    /// </remarks>
    public int ChunkSize => (int)NativeMethods.duckdb_vector_size();
}
