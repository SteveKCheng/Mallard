using System;
using Mallard.Interop;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;

namespace Mallard;

internal unsafe class DuckDbDatabase
{
    private _duckdb_database* _nativeDb;
    private int _refCount;

    public string Path { get; private set; }

    public ImmutableArray<KeyValuePair<string, string>> Options { get; }

    /// <summary>
    /// Connect to a DuckDB database.
    /// </summary>
    /// <param name="path">
    /// The path to the DuckDB database, in DuckDB's native syntax.
    /// </param>
    /// <param name="options">
    /// Options for opening the database.
    /// </param>
    /// <param name="database">
    /// The managed wrapper for the DuckDB database resource.
    /// This resource should be "released", by calling <see cref="Release" />,
    /// when the database is considered no longer in use.
    /// </param>
    /// <returns>
    /// Native handle to the DuckDB connection.
    /// </returns>
    internal static _duckdb_connection* Connect(string path,
                                                IEnumerable<KeyValuePair<string, string>>? options,
                                                out DuckDbDatabase database)
    {
        var d = new DuckDbDatabase(path, options);
        try
        {
            var c = d.ConnectExisting();
            database = d;
            return c;
        }
        catch
        {
            d.Release();
            throw;
        }
    }
    
    /// <summary>
    /// Re-connect to a DuckDB database given its managed object wrapper.
    /// </summary>
    /// <param name="database">
    /// The existing managed wrapper for the DuckDB database resource.
    /// If the native database resource has already been freed,
    /// this method will try to re-open the database using the same
    /// paths and options, and a new managed wrapper will be set
    /// into this argument on successful return.
    /// </param>
    /// <returns>
    /// Native handle to the DuckDB connection.
    /// </returns>
    internal static _duckdb_connection* Reconnect(ref DuckDbDatabase database)
    {
        var d = database;
        if (!d.TryAcquire())
            return Connect(d.Path, d.Options, out database);
        
        try
        {
            return d.ConnectExisting();
        }
        catch
        {
            d.Release();
            throw;
        }
    }

    private DuckDbDatabase(string path, IEnumerable<KeyValuePair<string, string>>? options)
    {
        duckdb_state status;

        _duckdb_config* nativeConfig = null;
        try
        {
            // Convert options
            if (options != null)
            {
                status = NativeMethods.duckdb_create_config(out nativeConfig);
                DuckDbException.ThrowOnFailure(status, "Could not create configuration object in native DuckDB library. ");
                foreach (var (key, value) in options)
                {
                    status = NativeMethods.duckdb_set_config(nativeConfig, key, value);
                    DuckDbException.ThrowOnFailure(status, "Could not set configuration option in native DuckDB library. ");
                }
            }

            Path = path;
            Options = options != null ? options.ToImmutableArray() 
                                      : ImmutableArray<KeyValuePair<string, string>>.Empty;

            status = NativeMethods.duckdb_open_ext(path, out _nativeDb, nativeConfig, out var errorString);
            DuckDbException.ThrowOnFailure(status, string.Empty);
        }
        finally
        {
            if (nativeConfig != null)
                NativeMethods.duckdb_destroy_config(ref nativeConfig);
        }

        _refCount = 1;
    }

    private _duckdb_connection* ConnectExisting()
    {
        var status = NativeMethods.duckdb_connect(_nativeDb, out var nativeConn);
        if (status != duckdb_state.DuckDBSuccess)
            throw new DuckDbException("Could not connect to database. ");

        return nativeConn;
    }

    private bool TryAcquire()
    {
        // Increment reference count unless it is currently <= 0.
        int v = _refCount;
        int w;
        do
        {
            if (v <= 0)
                return false;

            w = v;
            v = Interlocked.CompareExchange(ref _refCount, w + 1, w);
        } while (v != w);

        return true;
    }

    internal void Release()
    {
        if (Interlocked.Decrement(ref _refCount) == 0)
            NativeMethods.duckdb_close(ref _nativeDb);
    }
}
