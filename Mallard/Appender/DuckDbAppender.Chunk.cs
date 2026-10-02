using System;
using Mallard.Interop;

namespace Mallard;

public partial class DuckDbAppender
{
    /// <summary>
    /// A reusable native data chunk for bulk chunk-writing, created lazily on the first call to
    /// <see cref="AppendChunk{TState}" /> and reused (via <c>duckdb_data_chunk_reset</c>) thereafter.
    /// Null until first used; owned by this appender and destroyed on disposal.
    /// </summary>
    private unsafe _duckdb_data_chunk* _writeChunk;

    /// <summary>
    /// Cached type information for the columns of <see cref="_writeChunk" />, in declaration order.
    /// </summary>
    private DuckDbColumnInfo[]? _writeColumns;

    /// <summary>
    /// The capacity of <see cref="_writeChunk" />, i.e. <c>duckdb_vector_size()</c>.
    /// </summary>
    private int _writeCapacity;

    /// <summary>
    /// Append a batch of rows in bulk by writing directly into the memory of a DuckDB data chunk.
    /// </summary>
    /// <typeparam name="TState">
    /// Type of arbitrary state to pass into the caller-specified function.
    /// </typeparam>
    /// <param name="state">
    /// The state object or structure to pass into <paramref name="func" />.
    /// </param>
    /// <param name="func">
    /// The caller-specified function that populates the chunk's columns (through the
    /// <see cref="DuckDbChunkWriter" /> it is given) and returns the number of rows populated.
    /// </param>
    /// <returns>
    /// The number of rows appended, i.e. the value returned by <paramref name="func" />.  This is
    /// convenient for driving a loop that appends more rows than fit in a single chunk.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This is the most efficient way to insert many rows: rather than crossing the native boundary
    /// once per value (as the row-at-a-time <see cref="Append" /> / <see cref="FinishRow" /> API does),
    /// the caller writes up to <see cref="DuckDbChunkWriter.Capacity" /> rows straight into the
    /// column vectors' memory, and the whole chunk is handed to DuckDB in a single call.
    /// </para>
    /// <para>
    /// To insert more rows than fit in one chunk, call this method repeatedly.  The underlying data
    /// chunk is allocated once and reused across calls.
    /// </para>
    /// <para>
    /// In this initial implementation only "raw" writing of primitive, fixed-width columns is
    /// supported, and every appended row is valid (non-null).  Columns requiring conversion,
    /// variable-length columns (e.g. <c>VARCHAR</c>), nested columns, and null values are not yet
    /// supported through this API; use the row-at-a-time API for those.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="func" /> is null.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    /// This appender has been disposed.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// This appender has already reported an error from DuckDB and can no longer be used.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="func" /> returns a row count that is negative or greater than
    /// <see cref="DuckDbChunkWriter.Capacity" />.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// DuckDB reported an error while appending the chunk, e.g. due to a constraint violation.
    /// </exception>
    public unsafe int AppendChunk<TState>(TState state, DuckDbChunkWritingFunc<TState> func)
        where TState : allows ref struct
    {
        ArgumentNullException.ThrowIfNull(func);

        using var _ = _barricade.EnterScope(this);
        CheckNotFailed();
        EnsureWriteChunkInitialized();

        // Invalidate any outstanding Slot handed out by the row-wise API so a stale slot cannot
        // fire against this appender around a chunk write.
        _sequenceCounter++;

        // Invoke the user's chunk-writing function.
        //
        // The thread-exclusive lock is not relinquished.  In other contexts, holding locks
        // while calling out to arbitrary code is not advisable, but here, we must disallow the
        // user from making any mutation through other calls to this DuckDbAppender object while
        // still writing the chunk.  Since the barricade is non-reentrant we do not need to 
        // set and check another flag signaling this object is "in use".
        var writer = new DuckDbChunkWriter(_writeChunk, _writeColumns!, _writeCapacity);
        var rowCount = func(writer, state);

        try
        {
            if (unchecked((uint)rowCount > (uint)_writeCapacity))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(func),
                    $"The chunk-writing function returned a row count ({rowCount}) that is negative or " +
                    $"exceeds the chunk capacity ({_writeCapacity}). ");
            }

            NativeMethods.duckdb_data_chunk_set_size(_writeChunk, rowCount);
            var status = NativeMethods.duckdb_append_data_chunk(_nativeObj, _writeChunk);

            ThrowOnAppenderFailure(status, "Failed to append data chunk. ");
        }
        finally
        {
            // Reset the chunk for reuse on the next call.  Only meaningful if the append succeeded; on
            // failure the appender is marked failed below and cannot be used further anyway.
            //
            // Note that we do reset the chunk if the row count returned by the user above is invalid,
            // because we do not disable this object permanently when that happens (although we could).
            if (!_hasFailed)
                NativeMethods.duckdb_data_chunk_reset(_writeChunk);
        }

        return rowCount;
    }

    /// <summary>
    /// Lazily create the reusable data chunk and cache the column type information, by querying the
    /// appender for the columns of the table being appended to.
    /// </summary>
    /// <remarks>
    /// Must only be called while the current thread holds <see cref="_barricade" />.
    /// </remarks>
    private unsafe void EnsureWriteChunkInitialized()
    {
        if (_writeChunk != null)
            return;

        var columnCount = (int)NativeMethods.duckdb_appender_column_count(_nativeObj);
        if (columnCount <= 0)
            throw new DuckDbException("Could not determine the columns of the table being appended to. ");

        var columns = new DuckDbColumnInfo[columnCount];

        // Native logical-type handles for the chunk's columns.  DuckDB copies these during chunk
        // creation, so we own them and destroy them again in the finally block below.
        var nativeTypes = stackalloc _duckdb_logical_type*[columnCount];

        try
        {
            for (var i = 0; i < columnCount; ++i)
            {
                nativeTypes[i] = NativeMethods.duckdb_appender_column_type(_nativeObj, i);
                columns[i] = new DuckDbColumnInfo(nativeTypes[i]);
            }

            var chunk = NativeMethods.duckdb_create_data_chunk(nativeTypes, columnCount);
            if (chunk == null)
                throw new DuckDbException("DuckDB failed to create a data chunk for bulk appending. ");

            _writeChunk = chunk;
            _writeColumns = columns;
            _writeCapacity = (int)NativeMethods.duckdb_vector_size();
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
}
