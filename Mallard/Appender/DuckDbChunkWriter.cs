using Mallard.Interop;
using System;

namespace Mallard;

/// <summary>
/// Encapsulates user-defined code that populates the columns/vectors of a
/// <see cref="DuckDbChunkWriter" /> for bulk appending to DuckDB.
/// </summary>
/// <typeparam name="TState">
/// Type of arbitrary state that the user-defined code can take.  The state may be a "ref struct";
/// it is always passed in by value so the user-defined code cannot leak out dangling pointers into
/// the chunk (unless unsafe code is used).
/// </typeparam>
/// <param name="writer">
/// Gives (temporary) write access to the current chunk.
/// </param>
/// <param name="state">
/// Arbitrary state that the user-defined code can take.
/// </param>
/// <returns>
/// The number of rows that were populated into the chunk, which must be between 0 and
/// <see cref="DuckDbChunkWriter.Capacity" /> inclusive.  Exactly this many rows are appended
/// to the table.
/// </returns>
/// <remarks>
/// Returning the row count (rather than setting it through a property) means the C# compiler
/// enforces that it is always supplied: forgetting it is a compile error, not a silent run-time bug.
/// </remarks>
public delegate int DuckDbChunkWritingFunc<in TState>(in DuckDbChunkWriter writer, TState state)
    where TState : allows ref struct;

/// <summary>
/// Gives write access to the columns of a data chunk that is being filled for bulk appending
/// to a DuckDB table.
/// </summary>
/// <remarks>
/// <para>
/// This type is the write-side mirror of <see cref="DuckDbChunkReader" />.  Access is intermediated
/// through this "ref struct" so that pointers to the native memory of the chunk can only be used
/// within a restricted scope: an instance is passed to the body of a
/// <see cref="DuckDbChunkWritingFunc{TState}" /> and cannot escape it.
/// </para>
/// <para>
/// Populate the chunk by obtaining a writer for each column with <see cref="GetColumnRaw{T}" /> and
/// writing values into it, then return the number of rows written from the chunk-writing function.
/// </para>
/// </remarks>
public unsafe readonly ref struct DuckDbChunkWriter
{
    /// <summary>
    /// Borrowed handle for the native object of the data chunk.
    /// </summary>
    private readonly _duckdb_data_chunk* _nativeChunk;

    /// <summary>
    /// Cached type information for the columns of the chunk, in declaration order.
    /// </summary>
    private readonly DuckDbColumnInfo[] _columns;

    internal DuckDbChunkWriter(_duckdb_data_chunk* nativeChunk, DuckDbColumnInfo[] columns, int capacity)
    {
        _nativeChunk = nativeChunk;
        _columns = columns;
        Capacity = capacity;
    }

    /// <summary>
    /// The maximum number of rows that can be written into this chunk.
    /// </summary>
    /// <remarks>
    /// This is DuckDB's standard vector size (<c>duckdb_vector_size()</c>, normally 2048).  To append
    /// more rows than this, invoke <see cref="DuckDbAppender.AppendChunk{TState}" /> repeatedly.
    /// </remarks>
    public int Capacity { get; }

    /// <summary>
    /// The number of columns in this chunk, matching the columns of the table being appended to.
    /// </summary>
    public int ColumnCount => _columns.Length;

    /// <summary>
    /// Get write access to the raw data for one column of this chunk.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind the elements of the column to.  This type must be what
    /// <see cref="DuckDbVectorRawWriter{T}" /> accepts for the selected column.
    /// </typeparam>
    /// <param name="columnIndex">
    /// The index of the column.
    /// </param>
    /// <returns>
    /// <see cref="DuckDbVectorRawWriter{T}" /> for writing the data of the column.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="columnIndex"/> is out of range.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <typeparamref name="T" /> does not match the storage type of the selected column.
    /// </exception>
    public DuckDbVectorRawWriter<T> GetColumnRaw<T>(int columnIndex) where T : unmanaged, allows ref struct
        => new(GetVectorInfo(columnIndex));

    private DuckDbVectorInfo GetVectorInfo(int columnIndex)
    {
        if (unchecked((uint)columnIndex >= (uint)_columns.Length))
        {
            throw new ArgumentOutOfRangeException(
                nameof(columnIndex),
                "The specified column index does not refer to an existent column. ");
        }

        var nativeVector = NativeMethods.duckdb_data_chunk_get_vector(_nativeChunk, columnIndex);
        return new DuckDbVectorInfo(nativeVector, Capacity, _columns[columnIndex]);
    }
}
