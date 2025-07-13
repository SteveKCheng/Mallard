using Mallard.C_API;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Mallard;

internal unsafe class DuckDbDatabase
{
    private _duckdb_database* _nativeDb;
    private int _refCount;

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

public unsafe class DuckDbConnection : IDisposable, IRefCountedObject
{
    private _duckdb_connection* _nativeConn;

    private int _refCount;
    ref int IRefCountedObject.RefCount => ref _refCount;

    private DuckDbDatabase _database;

    /// <summary>
    /// Open a connection to a database from DuckDB.
    /// </summary>
    /// <param name="path">
    /// Specifies the location of the database as a path in DuckDB syntax.
    /// </param>
    /// <param name="options">
    /// Options for opening the database, as a sequence of key-value pairs.
    /// (All options in DuckDB are in string format.)
    /// </param>
    public DuckDbConnection(string path, IEnumerable<KeyValuePair<string, string>>? options = null)
    {
        var database = new DuckDbDatabase(path, options);
        try
        {
            _nativeConn = database.Connect();
        }
        catch
        {
            database.ReleaseRef();
            throw;
        }

        _database = database;
    }

    public long ExecuteNonQuery(string sql)
    {
        using var _ = this.UseRef();

        duckdb_state status;
        status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        try
        {
            if (status != duckdb_state.DuckDBSuccess)
                throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));

            var resultType = NativeMethods.duckdb_result_return_type(nativeResult);
            if (resultType == duckdb_result_type.DUCKDB_RESULT_TYPE_CHANGED_ROWS)
                return NativeMethods.duckdb_rows_changed(ref nativeResult);

            return 0;
        }
        finally
        {
            NativeMethods.duckdb_destroy_result(ref nativeResult);
        }
    }

    public DuckDbResult Execute(string sql)
    {
        using var _ = this.UseRef();

        duckdb_state status;
        status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
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

    public DuckDbConnection Reopen()
    {
        using var _ = this.UseRef();
        return new DuckDbConnection(_database);
    }

    private DuckDbConnection(DuckDbDatabase database)
    {
        _nativeConn = database.Connect();
        database.AcquireRef();
        _database = database;
    }

    private void DisposeImpl(bool disposing)
    {
        if (!this.PrepareToDispose())
            return;

        NativeMethods.duckdb_disconnect(ref _nativeConn);
        _database.ReleaseRef();
    }

    ~DuckDbConnection()
    {
        DisposeImpl(disposing: false);
    }

    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }
}
