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

public unsafe class DuckDbConnection : IDisposable
{
    private _duckdb_connection* _nativeConn;

    private HandleRefCount _refCount;
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

    /// <summary>
    /// Execute a SQL statement, and report only the number of rows changed.
    /// </summary>
    /// <returns>
    /// The number of rows changed by the execution of the statement.
    /// The result is -1 if the statement did not change any rows, or is otherwise
    /// a statement or query for which DuckDB does not report the number of rows changed.
    /// </returns>
    public long ExecuteNonQuery(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        return DuckDbResult.ExtractNumberOfChangedRows(status, ref nativeResult);
    }

    /// <summary>
    /// Execute a SQL statement and return the results (of the query).
    /// </summary>
    /// <returns>
    /// The results of the query execution.
    /// </returns>
    public DuckDbResult Execute(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        return DuckDbResult.CreateFromQuery(status, ref nativeResult);
    }

    public DuckDbConnection Reopen()
    {
        using var _ = _refCount.EnterScope(this);
        return new DuckDbConnection(_database);
    }

    private DuckDbConnection(DuckDbDatabase database)
    {
        _nativeConn = database.Connect();
        database.AcquireRef();
        _database = database;
    }

    #region Prepared statements

    // FIXME
    // The standard interface method System.Data.IDbConnection.CreateCommand() does not
    // take the SQL string as a parameter.  Instead it creates a mutable command object
    // where the SQL string can be set later.  This is not compatible with the way DuckDB
    // works, so eventually we have to implement a new DbCommand class that delays the
    // actual preparation of the command until execution happens.  Then, the current
    // DuckDbCommand class will probably be renamed to DuckDbPreparedStatement; and
    // we will offer a new (non-interface) method to create a prepared statement in
    // the DuckDB "native way", for efficiency.
    public DuckDbCommand CreatePreparedStatement(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        return new DuckDbCommand(_nativeConn, sql);
    }

    #endregion

    #region Resource management

    private void DisposeImpl(bool disposing)
    {
        if (!_refCount.PrepareToDisposeOwner())
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

    #endregion

    #region Global information

    private static string? _nativeLibraryVersion;

    /// <summary>
    /// The version of the native DuckDB library being used, as a string.
    /// </summary>
    public static string NativeLibraryVersion 
        => (_nativeLibraryVersion ??= NativeMethods.duckdb_library_version());

    #endregion

}
