using Mallard.C_API;
using System;
using System.Collections.Generic;

namespace Mallard;

public unsafe sealed partial class DuckDbConnection : IDisposable
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

    #region Executing statements from SQL strings

    /// <summary>
    /// Execute a SQL statement, and report only the number of rows changed.
    /// </summary>
    /// <param name="sql">
    /// SQL statement(s) in the DuckDB dialect.  Multiple statements
    /// may be separated/terminated by semicolons.  The number of rows
    /// changed is always from the last statement.
    /// </param>
    /// <returns>
    /// The number of rows changed by the execution of the statement.
    /// The result is -1 if the statement did not change any rows, or is otherwise
    /// a statement or query for which DuckDB does not report the number of rows changed.
    /// </returns>
    public long ExecuteNonQuery(string sql)
    {
        using var _ = _refCount.EnterScope(this);

        // Status can be ignored since any errors can be extracted from nativeResult
        NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);

        return DuckDbResult.TakeNumberOfChangedRows(ref nativeResult);
    }

    /// <summary>
    /// Execute a SQL statement and return the results (of the query).
    /// </summary>
    /// <param name="sql">
    /// SQL statement(s) in the DuckDB dialect.  Multiple statements
    /// may be separated/terminated by semicolons; the results returned
    /// are always from the last statement.
    /// </param>
    /// <returns>
    /// The results of the query execution.
    /// </returns>
    public DuckDbResult Execute(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        return DuckDbResult.CreateFromQuery(status, ref nativeResult);
    }

    /// <summary>
    /// Execute a SQL query, and return the first item in the results.
    /// </summary>
    /// <param name="sql">
    /// SQL statement(s) in the DuckDB dialect.  Multiple statements
    /// may be separated/terminated by semicolons; the result returned
    /// is always from the last statement.
    /// </param>
    /// <returns>
    /// The first row and cell of the results of the statement execution, if any.
    /// Null is returned if the statement does not produce any results.
    /// This method is typically for SQL statements that produce a single value.
    /// </returns>
    public object? ExecuteScalar(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        return DuckDbResult.ExtractFirstCell<object>(status, ref nativeResult);
    }

    /// <summary>
    /// Execute a SQL query, and return the first item in the results.
    /// </summary>
    /// <param name="sql">
    /// SQL statement(s) in the DuckDB dialect.  Multiple statements
    /// may be separated/terminated by semicolons; the result returned
    /// is always from the last statement.
    /// </param>
    /// <returns>
    /// <para>
    /// The first row and cell of the results of the statement execution, if any.
    /// This method is typically for SQL statements that produce a single value.
    /// </para>
    /// <para>
    /// The default value for <typeparamref name="T" /> is produced 
    /// when the SQL execution does not produce any results, unless
    /// the default value can be confused with a valid value, specifically
    /// when <typeparamref name="T" /> is a non-nullable value type.
    /// (This exception in behavior exists to avoid silently reading the
    /// wrong values.)  If <typeparamref name="T" /> is a reference type
    /// or nullable value type, the default value means "null".
    /// </para>
    /// </returns>
    public T? ExecuteValue<T>(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        return DuckDbResult.ExtractFirstCell<T>(status, ref nativeResult);
    }

    #endregion 

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

    #region Transactions

    /// <summary>
    /// Execute a command (SQL statement) in DuckDB and only check for errors.
    /// </summary>
    private void ExecuteCommand(string sql)
    {
        using var _ = _refCount.EnterScope(this);

        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);

        try
        {
            if (status != duckdb_state.DuckDBSuccess)
                throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));
        }
        finally
        {
            NativeMethods.duckdb_destroy_result(ref nativeResult);
        }
    }

    /// <summary>
    /// Begin a transaction on the current connection.
    /// </summary>
    /// <remarks>
    /// We do not need to check for an existing transaction being active because
    /// DuckDB already does that.
    /// </remarks>
    internal void BeginTransactionInternal() => ExecuteCommand("BEGIN TRANSACTION");

    /// <summary>
    /// Commit a transaction that was begun by <see cref="BeginTransaction" />.
    /// </summary>
    /// <remarks>
    /// We do not need to check for an existing transaction being active because
    /// DuckDB already does that.
    /// </remarks>
    internal void CommitTransactionInternal() => ExecuteCommand("COMMIT");

    /// <summary>
    /// Roll back a transaction that was begun by <see cref="BeginTransaction" />.
    /// </summary>
    /// <remarks>
    /// We do not need to check for an existing transaction being active because
    /// DuckDB already does that.
    /// </remarks>
    internal void RollbackTransactionInternal() => ExecuteCommand("ROLLBACK");

    /// <summary>
    /// Begin a transaction on the current connection.
    /// </summary>
    /// <returns>
    /// A "holder" object that is used to either commit the transaction or
    /// tp roll it back.  Put it in a <c>using</c> block in C#.
    /// </returns>
    public DuckDbTransaction BeginTransaction() => new DuckDbTransaction(this);

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
    private static string? _originalNativeLibraryVersion;

    /// <summary>
    /// The version of the native DuckDB library being used, as a string.
    /// </summary>
    public static string NativeLibraryVersion 
        => (_nativeLibraryVersion ??= NativeMethods.duckdb_library_version());

    /// <summary>
    /// The version of the native DuckDB library that Mallard has been built against.
    /// </summary>
    /// <remarks>
    /// In contrast to <see cref="NativeLibraryVersion" />, this property
    /// is available even when the native library has not been loaded yet.
    /// The reported version may be used to locate the DuckDB library to load
    /// (if the installation allows multiple versions of the library),
    /// and to check when an actual version of the library is API/ABI-compatible
    /// with what this version of Mallard was built against.  
    /// </remarks>
    public static string OriginalNativeLibraryVersion
        => (_originalNativeLibraryVersion ??= DuckDbVersionAttribute.Instance.Value);

    #endregion

}
