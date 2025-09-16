using Mallard.C_API;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Mallard;

/// <summary>
/// A connection to a DuckDB database.
/// </summary>
/// <remarks>
/// <para>
/// Mallard provides a high-performance API to access DuckDB through
/// this class and related types with the <c>DuckDb</c> prefix.
/// Alternatively, the standard ADO.NET API may be used which this
/// class also implements.
/// </para>
/// <para>
/// This class represents a connection to some DuckDB database.
/// Each connection is generally used only by at most one thread at once,
/// although it may be passed between different threads without issue.
/// </para>
/// <para>
/// This class is thread-safe in the sense that no memory or state
/// corruption will occur if an instance is used from multiple threads.
/// There are checks to prevent conflicting operations, such as disposing
/// an instance while it is being used by another thread.  However,
/// key operations such as making queries will be serialized (with locks
/// internally in DuckDB) if made from multiple threads.  
/// </para>
/// <para>
/// For true concurrent usage, make multiple connections to the same database.
/// DuckDB will mediate between the connections by its
/// optimistic multi-version concurrency control (MVCC).
/// </para>
/// <para>
/// There is no asynchronous interface, because DuckDB does not natively
/// offer one.  As an embedded database, most of wait time for SQL execution 
/// is (or is assumed to be) CPU-bound for the local computer,
/// not network-bound like traditional database servers.  This class does
/// not attempt to emulate asynchonicity since both the design and
/// implementation of the necessary API is non-trivial.  An asynchronous
/// interface could be built on top of this class instead.
/// </para>
/// </remarks>
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
    /// <exception cref="DuckDbException">
    /// Failed to open or connect to the database due to an invalid path,
    /// invalid configuration options, or other database-related error.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The SQL statement failed to execute due to a syntax error,
    /// constraint violation, or other database error.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The SQL statement failed to execute due to a syntax error,
    /// constraint violation, or other database error.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The SQL statement failed to execute due to a syntax error,
    /// constraint violation, or other database error.
    /// </exception>
    public object? ExecuteScalar(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        return DuckDbResult.ExtractFirstCell<object>(status, ref nativeResult);
    }

    /// <summary>
    /// Execute a SQL query, and return the first item in the results.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to convert the result (first item) from the SQL query to.
    /// </typeparam>
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
    /// <typeparamref name="T" /> is a non-nullable value type.
    /// (The special case in behavior exists because the default value
    /// of a non-nullable value type, such as <see cref="int" />,
    /// may be a valid value, which needs to be distinguished from a
    /// missing value.) If <typeparamref name="T" /> is a reference type
    /// or nullable value type, the default value means "null".
    /// </para>
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The SQL statement failed to execute due to a syntax error,
    /// constraint violation, or other database error.
    /// </exception>
    /// <exception cref="InvalidCastException">
    /// The first cell value cannot be converted to the type <typeparamref name="T" />.
    /// </exception>
    public T? ExecuteValue<T>(string sql)
    {
        using var _ = _refCount.EnterScope(this);
        var status = NativeMethods.duckdb_query(_nativeConn, sql, out var nativeResult);
        return DuckDbResult.ExtractFirstCell<T>(status, ref nativeResult);
    }

    /// <summary>
    /// Interrupt a query or statement, running in another thread, if any.
    /// </summary>
    /// <remarks>
    /// If multiple queries are queued up (by multiple threads calling methods like
    /// <see cref="Execute" />), only the first one is cancelled.
    /// </remarks> 
    public void Interrupt()
    {
        using var _ = _refCount.EnterScope(this);
        NativeMethods.duckdb_interrupt(_nativeConn);
    }

    #endregion 

    /// <summary>
    /// Open a new connection to the same database.
    /// </summary>
    /// <remarks>
    /// This method creates a new connection to the same database that this
    /// instance is connected to.  The new connection allows queries and statements
    /// to be submitted to the same database, in parallel, from a different thread.
    /// (If multiple threads attempt to execute statements on the same connection,
    /// execution will be serialized by locks inside DuckDB.)
    /// </remarks>
    /// <returns>
    /// A new connection to the same database.  
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// Failed to open or connect to the database due to an invalid path,
    /// invalid configuration options, or other database-related error.
    /// </exception>
    public DuckDbConnection Duplicate()
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

    /// <summary>
    /// Create a prepared statement for (parameterized) execution.
    /// </summary>
    /// <param name="sql">
    /// SQL statement(s) in the DuckDB dialect that may have formal parameters.
    /// Multiple statements may be separated/terminated by semicolons.
    /// </param>
    /// <returns>
    /// Object that holds the prepared statement from DuckDB.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The SQL statement failed to prepare due to a syntax error or other database error.
    /// </exception>
    public DuckDbStatement PrepareStatement(string sql)
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

    /// <summary>
    /// Disposes (closes) this database connection.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Native resources for the DuckDB connection will be released.
    /// </para>
    /// <para>
    /// This method does nothing if the connection is already disposed (closed).
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Another thread is using this connection, e.g. a query is still running
    /// on this connection.  The connection may not be disposed concurrently.
    /// </exception>
    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    #endregion

    #region Global information

    /// <summary>
    /// The version of the native DuckDB library being used.
    /// </summary>
    /// <value>
    /// The string representation of the DuckDB version.  For official releases, it will
    /// be in <a href="https://semver.org/">SemVer</a> format: <c>Major.Minor.Patch</c>.
    /// </value>
    [field: AllowNull]
    public static string NativeLibraryVersion 
        => (field ??= NativeMethods.duckdb_library_version());

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
    /// <value>
    /// The string representation of the DuckDB version.  For official releases, it will
    /// be in <a href="https://semver.org/">SemVer</a> format: <c>Major.Minor.Patch</c>.
    /// </value>
    [field: AllowNull]
    public static string OriginalNativeLibraryVersion
        => (field ??= DuckDbVersionAttribute.Instance.Value);

    #endregion
}
