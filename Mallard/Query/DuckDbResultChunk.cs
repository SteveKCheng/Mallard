using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// A chunk that has been retrieved (and captured) from the results of a query to DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// This class allows the same chunk to be processed or read over and over again.
/// Such functionality helps to implement row-oriented readers (whereas DuckDB is
/// naturally column-oriented), that implement methods returning individual rows.
/// </para>
/// <para>
/// For more efficient column-oriented, streaming processing, use methods like
/// <see cref="DuckDbResult.ProcessNextChunk" /> instead.
/// </para>
/// <para>
/// The data in the chunk is held by the native DuckDB library.  Instances of this class
/// should be disposed after the user is finished with a chunk, to avoid 
/// unpredictable memory usage (before garbage collection kicks in).
/// </para>
/// </remarks>
public unsafe class DuckDbResultChunk : IDisposable
{
    private _duckdb_data_chunk* _nativeChunk;
    private readonly IResultColumns _resultColumns;
    private readonly int _length;

    private HandleRefCount _refCount;

    /// <summary>
    /// Wrap a "chunk" object from the DuckDB native library.
    /// </summary>
    /// <param name="nativeChunk">
    /// Handle to the chunk from the DuckDB native library.  The new instance
    /// takes ownership of the handle.
    /// </param>
    /// <param name="columnInfo">
    /// Pre-tabulated information on the columns of the query result.
    /// </param>
    internal DuckDbResultChunk(ref _duckdb_data_chunk* nativeChunk,
                               IResultColumns resultColumns)
    {
        _nativeChunk = nativeChunk;
        nativeChunk = default;
        _resultColumns = resultColumns;
        _length = (int)NativeMethods.duckdb_data_chunk_get_size(_nativeChunk);
    }

    private void DisposeImpl(bool disposing)
    {
        if (!_refCount.PrepareToDisposeOwner())
            return;

        NativeMethods.duckdb_destroy_data_chunk(ref _nativeChunk);
    }

    ~DuckDbResultChunk()
    {
        DisposeImpl(disposing: false);
    }

    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <see cref="DuckDbResult.ColumnCount" />.
    public int ColumnCount => _resultColumns.ColumnCount;

    /// <see cref="DuckDbResult.GetColumnInfo" />.
    public DuckDbColumnInfo GetColumnInfo(int columnIndex) => _resultColumns.GetColumnInfo(columnIndex);

    /// <summary>
    /// The number of rows present in this chunk.
    /// </summary>
    public int Length => _length;

    /// <summary>
    /// Access the contents of this chunk of results in a direct, column-oriented manner.
    /// </summary>
    /// <typeparam name="TState">
    /// Type of arbitrary state to pass into the caller-specified function.
    /// </typeparam>
    /// <typeparam name="TReturn">
    /// The type of value returned by the caller-specified function.
    /// </typeparam>
    /// <param name="state">
    /// The state object or structure to pass into <paramref name="function" />.
    /// </param>
    /// <param name="function">
    /// The caller-specified function that receives the results from the next chunk
    /// and may do any processing on it.
    /// </param>
    /// <returns>
    /// Whatever <paramref name="function" /> returns.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method offers fast, direct access to the native memory backing the 
    /// DuckDB vectors (columns) of the results.  
    /// However, to make these operations safe (allowing no dangling pointers), 
    /// this library must be able to bound the scope of access.  Thus, the code
    /// to consume the vectors' data must be encapsulated in a function that this
    /// method invokes. 
    /// </para>
    /// <para>
    /// This method can be called over and over again, and the same data will be seen
    /// by the caller-specified function on each invocation.
    /// </para>
    /// </remarks>
    public TResult ProcessContents<TState, TResult>(TState state, DuckDbChunkReadingFunc<TState, TResult> func)
        where TState : allows ref struct
    {
        using var _ = _refCount.EnterScope(this);
        var reader = new DuckDbChunkReader(_nativeChunk, _resultColumns, _length);
        return func(reader, state);
    }

    internal DuckDbVectorReader<T> UnsafeGetColumnReader<T>(int columnIndex)
    {
        var vector = UnsafeGetColumnVector(columnIndex);
        var converter = _resultColumns.GetColumnConverter(columnIndex, typeof(T)).BindToVector(vector);
        return new DuckDbVectorReader<T>(vector, converter);
    }

    internal DuckDbVectorInfo UnsafeGetColumnVector(int columnIndex)
    {
        return DuckDbVectorInfo.FromNativeChunk(_nativeChunk, _resultColumns, Length, columnIndex);
    }
}
