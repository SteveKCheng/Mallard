using Mallard.C_API;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Mallard;

public unsafe sealed partial class DuckDbConnection : IDisposable
{
    /// <summary>
    /// Handle to the native DuckDB connection backing this instance. 
    /// </summary>
    /// <remarks>
    /// This is set to null when this instance has been completely disposed.
    /// </remarks>
    private _duckdb_connection* _nativeConn;

    /// <summary>
    /// Signals when this object is safe to be resurrected.   
    /// </summary>
    /// <remarks>
    /// <para>
    /// The value of this variable basically aligns with <see cref="_nativeConn" />
    /// being null.  However, we need to do an explicit "volatile" write as part
    /// of avoiding unsafe races between disposal and resurrection of this object.
    /// Unfortunately, C# does not allow that for pointer-typed variables,
    /// so we have to use a separate boolean variable.
    /// </para>
    /// <para>
    /// Note there is a short window of time between when <see cref="_refCount" />
    /// considers this instance to be disposed, and when this flag is set to true.
    /// In the middle of the disposal, this flag remains false.
    /// (This complicated dance is to avoid additional locking to accommodate
    /// re-opening connections, which we do not even recommend doing.)
    /// </para>
    /// </remarks>
    private bool _isSafeToResurrect;

    private HandleRefCount _refCount;
    
    /// <summary>
    /// Represents the underlying DuckDB database that this instance connects to. 
    /// </summary>
    /// <remarks>
    /// <para>
    /// There may be more than one connection on a single database, e.g. for issuing
    /// queries from multiple threads.  To make it easier to spin up new connections,
    /// the reference to the database object is retained.  The database object
    /// is not needed for the queries themselves.
    /// </para>
    /// <para>
    /// When this object is disposed, the additional reference count that this object
    /// holds on the database object is released (by <see cref="DuckDbDatabase.Release" />),
    /// which may entail releasing native resources.  But the managed database object
    /// (wrapper) is retained, so that a connection instance can be identified by the
    /// user even if disposed (by the file path the database has been opened by, etc.),
    /// and so that a connection can be easily re-opened. 
    /// </para>
    /// <para>
    /// Thus, re-connecting may change the database object but this member is never null
    /// (after construction).
    /// </para>
    /// </remarks>
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
        _nativeConn = DuckDbDatabase.Connect(path, options, out _database);
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
        _nativeConn = DuckDbDatabase.Reconnect(ref database);
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
    public DuckDbStatement CreatePreparedStatement(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        return new DuckDbStatement(_nativeConn, sql);
    }

    #endregion

    /// <summary>
    /// Execute a command (SQL statement) in DuckDB and only check for errors.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The caller must have acquired shared ownership of the native DuckDB resource.
    /// This method takes a dummy parameter to make that requirement clear. 
    /// </para>
    /// </remarks>
    private void ExecuteCommand(ref readonly HandleRefCount.Scope _, string sql)
    {
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
   
    #region Resource management

    private void DisposeImpl(bool disposing)
    {
        if (!_refCount.PrepareToDisposeOwner())
            return;

        // Get the database object reference first so we do not release the
        // wrong object in case this method races with resurrection from
        // IDbConnection.Open.  (The .NET memory model does not allow speculative
        // writes so the writing to the _database field in that other method
        // cannot be re-ordered to happen before _isSafeToResurrect is set to
        // true here.  And the "volatile" write / read barrier
        // below ensures this read here is not re-ordered to occur after.)
        var database = _database;

        NativeMethods.duckdb_disconnect(ref _nativeConn);
        
        // Invalidate any current transaction
        _transactionVersion = 0;
        
        Volatile.Write(ref _isSafeToResurrect, true);

        database.Release();
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
