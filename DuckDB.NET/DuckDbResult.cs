using DuckDB.C_API;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace DuckDB;

public unsafe sealed class DuckDbResult : IDisposable
{
    private readonly Lock _mutex = new();
    private duckdb_result _nativeResult;
    private bool isDisposed;
    private readonly ColumnInfo[] _columnInfo;

    internal readonly struct ColumnInfo
    {
        public string Name { get; init; }

        public duckdb_type BasicType { get; init; }
    }

    internal DuckDbResult(ref duckdb_result nativeResult)
    {
        _nativeResult = nativeResult;
        nativeResult = default;

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

    public duckdb_type GetColumnBasicType(int columnIndex) => _columnInfo[columnIndex].BasicType;
}

public unsafe class DuckDbResultChunk : IRefCountedObject, IDisposable
{
    private _duckdb_data_chunk* _nativeChunk;
    private readonly DuckDbResult.ColumnInfo[] _columnInfo;
    private readonly int _length;

    private int _refCount;
    ref int IRefCountedObject.RefCount => ref _refCount;

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
        if (!this.PrepareToDispose())
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
            using var _ = this.UseRef();
            return NativeMethods.duckdb_data_chunk_get_column_count(_nativeChunk);
        }
    }

    public int Length => _length;

    public TResult ProcessContents<TState, TResult>(TState state, DuckDbChunkReadingFunc<TState, TResult> func)
    {
        using var _ = this.UseRef();
        var reader = new DuckDbChunkReader(_nativeChunk, _columnInfo, _length);
        return func(reader, state);
    }
}

