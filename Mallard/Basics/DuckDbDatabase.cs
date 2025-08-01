using Mallard.C_API;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;

namespace Mallard;

internal unsafe class DuckDbDatabase
{
    private _duckdb_database* _nativeDb;
    private int _refCount;

    internal string Path { get; private set; }

    internal ImmutableArray<KeyValuePair<string, string>> Options { get; private set; }

    public DuckDbDatabase(string path, IEnumerable<KeyValuePair<string, string>>? options)
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

            status = NativeMethods.duckdb_open_ext(path, out _nativeDb, nativeConfig, out var errorString);
            DuckDbException.ThrowOnFailure(status, string.Empty);
        }
        finally
        {
            if (nativeConfig != null)
                NativeMethods.duckdb_destroy_config(ref nativeConfig);
        }

        _refCount = 1;

        Path = path;
        Options = options != null ? options.ToImmutableArray() : default;
    }

    internal _duckdb_connection* Connect()
    {
        var status = NativeMethods.duckdb_connect(_nativeDb, out var nativeConn);
        if (status != duckdb_state.DuckDBSuccess)
            throw new DuckDbException("Could not connect to database. ");

        return nativeConn;
    }

    internal void AcquireRef()
    {
        Interlocked.Increment(ref _refCount);
    }

    internal void ReleaseRef()
    {
        if (Interlocked.Decrement(ref _refCount) == 0)
            NativeMethods.duckdb_close(ref _nativeDb);
    }
}
