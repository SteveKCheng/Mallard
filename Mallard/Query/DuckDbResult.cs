using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Mallard;

public unsafe sealed class DuckDbResult : IDisposable
{
    private readonly Lock _mutex = new();
    private duckdb_result _nativeResult;
    private bool isDisposed;
    private readonly ColumnInfo[] _columnInfo;

    internal readonly struct ColumnInfo
    {
        public string Name { get; init; }

        public DuckDbBasicType BasicType { get; init; }
    }

    /// <summary>
    /// Wrap the native result from DuckDB, and handle errors. 
    /// </summary>
    /// <remarks>
    /// This code is common to prepared and non-prepared queries.
    /// </remarks>
    /// <param name="status">
    /// Return status from executing a query in DuckDB. 
    /// </param>
    /// <param name="nativeResult">
    /// The result of the query.  The caller loses ownership of this object: it is either
    /// transferred to the new instance of <see cref="DuckDbResult" />, or otherwise (when this
    /// method throws an exception) gets destroyed.
    /// </param>
    /// <returns>
    /// If <paramref name="status" /> indicates success, an instance of <see cref="DuckDbResult" />
    /// that wraps the native result object.
    /// </returns>
    internal static DuckDbResult CreateFromQuery(duckdb_state status, ref duckdb_result nativeResult)
    {
        try
        {
            if (status != duckdb_state.DuckDBSuccess)
                throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));

            // Passes ownership of nativeResult
            return new DuckDbResult(ref nativeResult);
        }
        catch
        {
            NativeMethods.duckdb_destroy_result(ref nativeResult);
            throw;
        }
    }

    /// <summary>
    /// Extract the number of changed rows from executing some SQL statement, and
    /// abandon the native result object.
    /// </summary>
    /// <remarks>
    /// This method is common code used to implement <see cref="DuckDbConnection.ExecuteNonQuery" />
    /// and <see cref="DuckDbCommand.ExecuteNonQuery" />.
    /// </remarks>
    internal static long ExtractNumberOfChangedRows(duckdb_state status, ref duckdb_result nativeResult)
    {
        try
        {
            if (status != duckdb_state.DuckDBSuccess)
                throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));
            var resultType = NativeMethods.duckdb_result_return_type(nativeResult);
            if (resultType == duckdb_result_type.DUCKDB_RESULT_TYPE_CHANGED_ROWS)
                return NativeMethods.duckdb_rows_changed(ref nativeResult);
            return -1;
        }
        finally
        {
            NativeMethods.duckdb_destroy_result(ref nativeResult);
        }
    }

    /// <summary>
    /// Extract the value at the first row and column, if it exists.
    /// </summary>
    /// <remarks>
    /// Used to implement <see cref="DuckDbConnection.ExecuteScalar(string)" />
    /// and similar methods.
    /// </remarks>
    internal static T? ExtractFirstCell<T>(duckdb_state status, ref duckdb_result nativeResult)
    {
        try
        {
            if (status != duckdb_state.DuckDBSuccess)
                throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));
            var resultType = NativeMethods.duckdb_result_return_type(nativeResult);
            if (resultType != duckdb_result_type.DUCKDB_RESULT_TYPE_QUERY_RESULT)
                return default;

            var nativeChunk = NativeMethods.duckdb_fetch_chunk(nativeResult);
            if (nativeChunk == null)
                return default;

            try
            {
                var length = (int)NativeMethods.duckdb_data_chunk_get_size(nativeChunk);
                if (length <= 0)
                    return default;

                var nativeVector = NativeMethods.duckdb_data_chunk_get_vector(nativeChunk, 0);
                if (nativeVector == null)
                    return default;

                var basicType = NativeMethods.duckdb_column_type(ref nativeResult, 0);
                var vectorInfo = new DuckDbVectorInfo(nativeVector, basicType, length);

                var reader = new DuckDbVectorReader<T>(vectorInfo);
                reader.TryGetItem(0, out var item);
                return item;
            }
            finally
            {
                NativeMethods.duckdb_destroy_data_chunk(ref nativeChunk);
            }
        }
        finally
        {
            NativeMethods.duckdb_destroy_result(ref nativeResult);
        }
    }

    private DuckDbResult(ref duckdb_result nativeResult)
    {
        _nativeResult = nativeResult;

        var columnCount = NativeMethods.duckdb_column_count(ref _nativeResult);

        _columnInfo = new ColumnInfo[columnCount];
        for (long i = 0; i < columnCount; ++i)
        {
            _columnInfo[i] = new ColumnInfo
            {
                Name = NativeMethods.duckdb_column_name(ref _nativeResult, i),
                BasicType = NativeMethods.duckdb_column_type(ref _nativeResult, i)
            };
        }

        // Ownership transfer
        nativeResult = default;
    }

    private void DisposeImpl(bool disposing)
    {
        lock (_mutex)
        {
            if (isDisposed)
                return;

            isDisposed = true;
            NativeMethods.duckdb_destroy_result(ref _nativeResult);
        }
    }

    ~DuckDbResult()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        DisposeImpl(disposing: false);
    }

    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    private void ThrowIfDisposed()
    {
        if (isDisposed)
            throw new ObjectDisposedException("Cannot operate on this object after it has been disposed. ");
    }

    public DuckDbResultChunk? FetchNextChunk()
    {
        _duckdb_data_chunk* nativeChunk;
        lock (_mutex)
        {
            ThrowIfDisposed();
            nativeChunk = NativeMethods.duckdb_fetch_chunk(_nativeResult);
        }

        if (nativeChunk == null)
            return null;    // exhausted all results

        try
        {
            return new DuckDbResultChunk(ref nativeChunk, _columnInfo);
        }
        catch
        {
            NativeMethods.duckdb_destroy_data_chunk(ref nativeChunk);
            throw;
        }
    }

    public bool ProcessNextChunk<TState, TResult>(TState state, 
                                                  DuckDbChunkReadingFunc<TState, TResult> action,
                                                  [MaybeNullWhen(false)] out TResult result)
    {
        _duckdb_data_chunk* nativeChunk;
        lock (_mutex)
        {
            ThrowIfDisposed();
            nativeChunk = NativeMethods.duckdb_fetch_chunk(_nativeResult);
        }

        if (nativeChunk == null)
        {
            result = default;
            return false;
        }

        try
        {
            var length = (int)NativeMethods.duckdb_data_chunk_get_size(nativeChunk);
            var reader = new DuckDbChunkReader(nativeChunk, _columnInfo, length);
            result = action(reader, state);
            return true;
        }
        finally
        {
            NativeMethods.duckdb_destroy_data_chunk(ref nativeChunk);
        }
    }

    public int ColumnCount => _columnInfo.Length;

    public string GetColumnName(int columnIndex) => _columnInfo[columnIndex].Name;

    public DuckDbBasicType GetColumnBasicType(int columnIndex) => _columnInfo[columnIndex].BasicType;
}

public unsafe class DuckDbResultChunk : IDisposable
{
    private _duckdb_data_chunk* _nativeChunk;
    private readonly DuckDbResult.ColumnInfo[] _columnInfo;
    private readonly int _length;

    private HandleRefCount _refCount;

    internal DuckDbResultChunk(ref _duckdb_data_chunk* nativeChunk,
                               DuckDbResult.ColumnInfo[] columnInfo)
    {
        _nativeChunk = nativeChunk;
        nativeChunk = default;
        _columnInfo = columnInfo;
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

    public long ColumnCount
    {
        get
        {
            using var _ = _refCount.EnterScope(this);
            return NativeMethods.duckdb_data_chunk_get_column_count(_nativeChunk);
        }
    }

    public int Length => _length;

    public TResult ProcessContents<TState, TResult>(TState state, DuckDbChunkReadingFunc<TState, TResult> func)
    {
        using var _ = _refCount.EnterScope(this);
        var reader = new DuckDbChunkReader(_nativeChunk, _columnInfo, _length);
        return func(reader, state);
    }
}

