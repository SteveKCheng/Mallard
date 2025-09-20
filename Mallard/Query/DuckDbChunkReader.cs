using Mallard.Interop;
using System;

namespace Mallard;

/// <summary>
/// Encapsulates user-defined code that accesses <see cref="DuckDbChunkReader" />
/// and its columns/vectors.
/// </summary>
/// <typeparam name="TState">
/// Type of arbitrary state that the user-defined code can take.  The state may be represented by 
/// a "ref struct", although it is always passed in by value so that it is impossible
/// for the user-defined code to leak out dangling pointers to inside the current chunk
/// (unless unsafe code is used).
/// </typeparam>
/// <typeparam name="TReturn">
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
public delegate TReturn DuckDbChunkReadingFunc<in TState, out TReturn>(in DuckDbChunkReader chunk, TState state)
    where TState : allows ref struct;

/// <summary>
/// Gives access to the data within a result chunk.
/// </summary>
/// <remarks>
/// <para>
/// Access must be intermediated through this "ref struct" to ensure that pointers to memory
/// allocated by the DuckDB native library can only be read from within a restricted scope.
/// This "ref struct" is passed to the body of 
/// <see cref="DuckDbChunkReadingFunc{TState, TReturn}" /> and cannot be accessed outside of that
/// body.
/// </para>
/// <para>
/// "Ref structs" cannot be ported across threads, which also increases performance, by 
/// making unnecessary all thread-safety checks when accessing a chunk through methods here.
/// </para>
/// </remarks>
public unsafe readonly ref struct DuckDbChunkReader
{
    /// <summary>
    /// Borrowed handle for the native object of the result chunk.
    /// </summary>
    private readonly _duckdb_data_chunk* _nativeChunk;

    /// <summary>
    /// Used to access type information on the columns.
    /// </summary>
    private readonly IResultColumns _resultColumns;

    /// <summary>
    /// Wrap the native object for a result chunk.
    /// </summary>
    /// <param name="nativeChunk">
    /// Handle to the native object.  This handle is borrowed for the duration of the scope 
    /// of this ref struct.
    /// </param>
    /// <param name="resultColumns">
    /// Used to access type information on the columns.
    /// </param>
    /// <param name="length">
    /// The length (number of rows) present in the chunk.  This value is cached into 
    /// the <see cref="Length" /> property, so no further API calls are needed to retrieve it.
    /// </param>
    internal DuckDbChunkReader(_duckdb_data_chunk* nativeChunk,
                               IResultColumns resultColumns,
                               int length)
    {
        _nativeChunk = nativeChunk;
        _resultColumns = resultColumns;
        Length = length;
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
        var vector = GetVectorInfo(columnIndex);
        var converter = _resultColumns.GetColumnConverter(columnIndex, typeof(T))
                                      .BindToVector(vector);

        return new DuckDbVectorReader<T>(vector, converter);
    }

    /// <summary>
    /// Get access to the raw data for one column for all the rows represented by this chunk.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind the elements of the column to.  This type must be 
    /// what <see cref="DuckDbVectorRawReader{T}" /> accepts for the selected column.
    /// </typeparam>
    /// <param name="columnIndex">
    /// The index of the column.
    /// </param>
    /// <returns>
    /// <see cref="DuckDbVectorRawReader{T}" /> representing the data for the column.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="columnIndex"/> is out of range, or this instance is default-initialized.
    /// </exception>
    public DuckDbVectorRawReader<T> GetColumnRaw<T>(int columnIndex) where T : unmanaged, allows ref struct
        => new(GetVectorInfo(columnIndex));

    /// <summary>
    /// Get the descriptor for a column's vector, common to both <see cref="DuckDbVectorReader{T}" />
    /// and <see cref="DuckDbVectorRawReader{T}" />.
    /// </summary>
    /// <param name="columnIndex">
    /// The index of the DuckDB column to select.
    /// </param>
    private DuckDbVectorInfo GetVectorInfo(int columnIndex)
        => DuckDbVectorInfo.FromNativeChunk(_nativeChunk, _resultColumns, Length, columnIndex);

    /// <summary>
    /// The length (number of rows) present in this chunk.
    /// </summary>
    public int Length { get; }
}
