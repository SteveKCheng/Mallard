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

    /// <summary>
    /// If true, requests to dispose (i.e. calls to <see cref="Dispose" />) are ignored.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Disabling explicit, immediate disposal is required to make APIs that read 
    /// DuckDB results <em>without using ref structs</em> thread safe, unless
    /// we are willing to introduce reference-counting everywhere.
    /// </para>
    /// <para>
    /// When an object implementing such an API holds a reference to a <see cref="DuckDbResultChunk" />
    /// (this object), some other thread might try to dispose the latter while the former
    /// is in the middle of the method that uses the latter.
    /// </para>
    /// <para>
    /// This library normally prevents such a situation by atomically increasing a reference count
    /// before all usages of objects with native resources, i.e. the same as what "safe handles" do
    /// in Microsoft's API designs.  But these atomic operations are expensive if they have to
    /// be performed for access to every cell in the query results.
    /// </para>
    /// <para>
    /// That means we are relying solely on the .NET garbage collector to call finalizers to
    /// clean up resources.  Hopefully that will be sufficient; if the garbage collector
    /// cannot "keep up", we would have to implement QSBR-based clean-up (scheduling clean-ups 
    /// by tracking quiescent states on all threads that use <see cref="DuckDbResultChunk" />),
    /// which is a lot more complicated.
    /// </para>
    /// </remarks>
    private bool _ignoreDisposals;

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

    /// <summary>
    /// Ignore all future requests to explicitly 
    /// dispose (via <see cref="Dispose" /> or <see cref="IDisposable.Dispose" />)
    /// for thread safety.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Once this method is called, its effects cannot be reversed for this object.
    /// </para>
    /// <para>
    /// A successful call of this method ensures that the current instance is always available
    /// for use as long as a caller holds a reference to it.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">
    /// This object has already been disposed (before disposals can be ignored).
    /// </exception>
    internal void IgnoreDisposals()
    {
        if (_ignoreDisposals)
            return;

        _refCount.PreventDispose(this);
        _ignoreDisposals = true;
    }

    private void DisposeImpl(bool disposing)
    {
        // N.B. The _ignoreDisposals flag is not integrated with _refCount, so
        // another thread could race to set the former (to true), and this test could fail
        // to see the new value before proceeding to attempt to mark this object for disposal.
        // But since _refCount.PreventDispose is called first before that flag is set,
        // at worst _refCount.PrepareToDisposeOwner would fail (with an exception), when
        // a silent return would be ideal.
        //
        // Such a race can only arise from incorrect usage patterns from client code, 
        // so causing failure (without any memory corruption) is fine.
        if (disposing && _ignoreDisposals)
            return;

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
