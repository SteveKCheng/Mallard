using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// Encapsulates user-defined code that accesses <see cref="DuckDbChunkReader" />.
/// </summary>
/// <typeparam name="TState">
/// Type of arbitrary state that the user-defined code can take.  The state may be represented by 
/// a "ref struct", although it is always passed in by value so that it is impossible
/// for the user-defined code to leak out dangling pointers to inside the current chunk
/// (unless unsafe code is used).
/// </typeparam>
/// <typeparam name="TResult">
/// Arbitrary return type from the user-defined function.  This type may not be a
/// "ref struct", to prevent dangling pointers to inside the current chunk from being
/// returned.  (The presence of the return value is for convenience; even if this delegate
/// has been defined to return void, user-defined code can still propagate results
/// out of the function by assigning to "ref" members suitably defined  
/// inside <typeparamref name="TState" />.)
/// </typeparam>
/// <param name="chunk">
/// Gives (temporary) access to the current chunk.
/// </param>
/// <param name="state">
/// Arbitrary state that the user-defined code can take.
/// </param>
/// <returns>
/// Whatever is desired.  Typically the return value would be the 
/// result of some transformation in the chunk's data.
/// </returns>
public delegate TResult DuckDbChunkReadingFunc<in TState, out TResult>(in DuckDbChunkReader chunk, TState state)
    where TState : allows ref struct;

/// <summary>
/// Gives access to the data within a result chunk.
/// </summary>
/// <remarks>
/// Access must be intermediated through this "ref struct" to ensure that pointers to memory
/// allocated by the DuckDB native library can only be read from within a restricted scope.
/// This "ref struct" is passed to the body of 
/// <see cref="DuckDbResultChunkFunc{TState}" /> and cannot be accessed outside of that
/// body.
/// </remarks>
public unsafe readonly ref struct DuckDbChunkReader
{
    private readonly _duckdb_data_chunk* _nativeChunk;
    private readonly DuckDbResult.ColumnInfo[] _columnInfo;
    private readonly int _length;

    internal DuckDbChunkReader(_duckdb_data_chunk* nativeChunk,
                               DuckDbResult.ColumnInfo[] columnInfo,
                               int length)
    {
        _nativeChunk = nativeChunk;
        _columnInfo = columnInfo;
        _length = length;
    }

    /// <summary>
    /// Get access to the data for one column for all the rows represented by this chunk.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind the elements of the column to.  This type must be compatible with
    /// the actual (dynamic) type of the column in DuckDB.
    /// </typeparam>
    /// <param name="columnIndex">
    /// The index of the column.
    /// </param>
    /// <returns>
    /// <see cref="DuckDbVectorReader{T}" /> representing the data for the column.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="columnIndex"/> is out of range, or this instance is default-initialized.
    /// </exception>
    public DuckDbVectorReader<T> GetColumn<T>(int columnIndex)
    {
        // In case the user calls this method on a default-initialized instance,
        // the native library will not crash on this call because it does
        // check _nativeChunk for null first, returning null in that case.
        var nativeVector = NativeMethods.duckdb_data_chunk_get_vector(_nativeChunk,
                                                                      columnIndex);
        if (nativeVector == null)
            throw new IndexOutOfRangeException("Column index is not in range. ");
        return new DuckDbVectorReader<T>(nativeVector, _columnInfo[columnIndex].BasicType, _length);
    }

    /// <summary>
    /// The length (number of rows) present in this chunk.
    /// </summary>
    public int Length => _length;
}
