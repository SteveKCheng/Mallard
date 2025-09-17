using System;
using System.Data;
using System.Threading;

namespace Mallard;

/// <summary>
/// Controls a database transaction in DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// An instance is obtained by <see cref="DuckDbConnection.BeginTransaction" />.
/// It should be put as the subject of a <c>using</c> block in C#, and within
/// that <c>using</c> block, statements may be submitted to the 
/// originating <see cref="DuckDbConnection" /> object.
/// </para>
/// <para>
/// This object should not be accessed from multiple threads at the same time.
/// (If that is done, exceptions will be thrown when attempting to commit
/// or roll back the transaction.)
/// </para>
/// <para>
/// This type is a value type only to avoid GC allocation.  All of the transaction
/// state is actually lives inside <see cref="DuckDbConnection" />.  (A DuckDB connection
/// can only accept at most one transaction at once.)
/// </para>
/// </remarks>
public readonly struct DuckDbTransaction : IDbTransaction, IEquatable<DuckDbTransaction>
{
    /// <summary>
    /// The database connection that this transaction was created on.
    /// </summary>
    public DuckDbConnection Connection { get; }

    IDbConnection? IDbTransaction.Connection => Connection;

    /// <summary>
    /// Version number of this transaction assigned by <see cref="DuckDbConnection" />,
    /// used for run-time checking of correctness.
    /// </summary>
    private int Version { get; }

    /// <summary>
    /// The isolation level of the database transaction.
    /// </summary>
    /// <remarks>
    /// DuckDB only supports snapshot-level isolation (<see cref="IsolationLevel.Snapshot" />
    /// and that is what this property always reports.
    /// </remarks>
    public IsolationLevel IsolationLevel => IsolationLevel.Snapshot;

    internal DuckDbTransaction(DuckDbConnection connection, int version)
    {
        Connection = connection;
        Version = version;
    }

    /// <summary>
    /// Commits this database transaction.
    /// </summary>
    /// <remarks>
    /// This transaction becomes inactive (same as its disposed state)
    /// once this method is called, whether it succeeds or fails.
    /// </remarks> 
    /// <exception cref="ObjectDisposedException">
    /// The underlying connection has already been disposed (closed). 
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// This transaction cannot be committed because it is no longer active
    /// (e.g. it has already been committed or rolled back).
    /// </exception>
    /// <exception cref="DuckDbException">
    /// There has been a database-level error in committing the transaction, e.g.
    /// another connection made changes to the database which causes conflict
    /// with the changes from this transaction.  
    /// </exception>
    public void Commit() => Connection.CommitTransaction(Version);

    /// <summary>
    /// Rolls back changes from this database transaction.
    /// </summary>
    /// <remarks>
    /// This transaction becomes inactive (same as its disposed state)
    /// once this method is called, whether it succeeds or fails.
    /// </remarks> 
    /// <exception cref="ObjectDisposedException">
    /// The underlying connection has already been disposed (closed). 
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// This transaction cannot be rolled back because it is no longer active
    /// (e.g. it has already been committed or rolled back).
    /// </exception>
    /// <exception cref="DuckDbException">
    /// There has been a database-level error in rolling back the transaction.
    /// </exception>
    public void Rollback() => Connection.RollbackTransaction(Version);

    /// <summary>
    /// Disposes of the transaction, equivalent to rolling it back if 
    /// it has not been committed or rolled back already.
    /// </summary>
    public void Dispose() => Connection.DropTransaction(Version);

    /// <summary>
    /// Whether this instance and the other instance refers to the same transaction
    /// on the same database. 
    /// </summary>
    public bool Equals(DuckDbTransaction other) 
        => Connection == other.Connection && Version == other.Version;

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is DuckDbTransaction other && Equals(other);
    
    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(Connection.GetHashCode(), Version);
}

public sealed partial class DuckDbConnection
{
    #region Transactions

    /// <summary>
    /// Register and begin a transaction on the current connection.
    /// </summary>
    private int RegisterTransaction()
    {
        using var scope = _refCount.EnterScope(this);
        
        // Allocate a version number for the new transaction.
        int currentVersion = _transactionVersion;
        int oldVersion;
        int newVersion;
        do
        {
            if (currentVersion != 0)
                throw new InvalidOperationException("A transaction has already started (and cannot be nested). ");

            oldVersion = currentVersion;
            
            // Increment version and take care of wrap-around
            newVersion = unchecked(currentVersion + 1);
            if (newVersion < 0) newVersion = 1;
            
            currentVersion = Interlocked.CompareExchange(ref _transactionVersion, newVersion, oldVersion);
        } while (currentVersion != oldVersion);

        try
        {
            ExecuteCommand(in scope, "BEGIN TRANSACTION"u8);
        }
        catch
        {
            _transactionVersion = 0;
            throw;
        }

        return newVersion;
    }

    /// <summary>
    /// Commit a transaction that was begun by <see cref="RegisterTransaction" />.
    /// </summary>
    internal void CommitTransaction(int version)
    {
        using var scope = _refCount.EnterScope(this);
        
        VerifyTransaction(in scope, version);
        try
        {
            ExecuteCommand(in scope, "COMMIT"u8);
        }
        finally
        {
            _transactionVersion = 0;
        }
    }
 
    /// <summary>
    /// Roll back a transaction that was begun by <see cref="RegisterTransaction" />.
    /// </summary>
    internal void RollbackTransaction(int version)
    {
        using var scope = _refCount.EnterScope(this);

        VerifyTransaction(in scope, version);
        try
        {
            ExecuteCommand(in scope, "ROLLBACK"u8);
        }
        finally
        {
            _transactionVersion = 0;
        }
    }

    /// <summary>
    /// Dispose of a transaction that was begun by <see cref="RegisterTransaction" />,
    /// meaning that it should be rolled back if it was not committed or rolled back
    /// already.
    /// </summary>
    internal void DropTransaction(int version)
    {
        // Okay to speculatively read without _refCount.EnterScope,
        // thus avoiding an unnecessary interlocked operation.
        //
        // Should this object get disposed (possibly from another thread racing),
        // the transaction cannot be valid anyway.
        //
        // Also, version == 0 should not happen except if the user attempts to
        // dispose a default-initialized DuckDbTransaction struct, which we
        // should ignore.
        if (version == 0 || version != _transactionVersion)
            return;

        try
        {
            RollbackTransaction(version);
        }
        catch (ObjectDisposedException)
        {
            // If the database connection was closed first, presumably roll-back has
            // already occurred (because the changes were not already committed),
            // so it should be safe to silently ignore the error.
            //
            // We still report any other kind of error, including racing with
            // transaction rollbacks from another thread.
        }
    }

    /// <summary>
    /// Tracks whether a transaction is active.
    /// </summary>
    /// <remarks>
    /// <para> 
    /// To prevent misuse of <see cref="DuckDbTransaction" />, we track the presence of a transaction
    /// in this object (the database connection), along with a version (to make sure old 
    /// instances of <see cref="DuckDbTransaction" /> are not being used). 
    /// </para>
    /// <para>
    /// This simple scheme can work, of course, because
    /// DuckDB only supports at most one transaction per database connection.
    /// </para>
    /// <para>
    /// A value of zero means there is no current transaction.  Positive values are
    /// versions assigned to new instances of <see cref="DuckDbTransaction" />.
    /// Negative values are not used.
    /// </para>
    /// <para>
    /// This variable has "shared ownership" in the same manner as <see cref="_nativeConn" />
    /// that is controlled by <see cref="_refCount" />.
    /// </para>
    /// <para>
    /// In theory, this variable should atomically flag when commit or rollback has started
    /// but not yet completed, to detect when the same transaction is being committed/rolled
    /// back at the same time from different threads.  However, this kind of error can pretty
    /// much only occur on the user deliberately provoking it; transaction objects are normally
    /// local to a function (and not shared between threads simultaneously).  Moreover,
    /// when such a race occurs, DuckDB's own error checking will catch it and so there will
    /// be no state/memory corruption even without the atomic flagging.  So we avoid
    /// the atomic flagging for a little extra efficiency and to simplify the code.  The only 
    /// minor drawback is that reported error may be attributed to the wrong transaction object.  
    /// </para>
    /// </remarks>
    private int _transactionVersion;

    /// <summary>
    /// Verify the version of a transaction matches the current state of this connection. 
    /// </summary>
    private void VerifyTransaction(ref readonly HandleRefCount.Scope _, int version)
    {
        if (version == 0 || version != _transactionVersion)
            throw new InvalidOperationException("Cannot commit or roll back a transaction that is no longer active. ");
    }

    /// <summary>
    /// Get the current transaction, to report for the <see cref="IDbCommand.Transaction" /> property.
    /// </summary>
    internal bool TryGetCurrentTransaction(out DuckDbTransaction transaction)
    {
        using var scope = _refCount.EnterScope(this);
        
        int version = _transactionVersion;
        if (version == 0)
        {
            transaction = default;
            return false;
        }
        else
        {
            transaction = new DuckDbTransaction(this, version);
            return true;
        }
    }

    /// <summary>
    /// Begin a transaction on the current connection.
    /// </summary>
    /// <returns>
    /// A "holder" object that is used to either commit the transaction or
    /// to roll it back.  In C#, wrap it in a <c>using</c> block or statement.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// This connection has been disposed.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// There is already an existing transaction for this connection.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// A database-level error occurred in starting the transaction.
    /// </exception>
    public DuckDbTransaction BeginTransaction()
        => new DuckDbTransaction(this, RegisterTransaction());

    #endregion
    
    #region Transactions: ADO.NET compatibility
    
    IDbTransaction IDbConnection.BeginTransaction() => BeginTransaction();

    IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il)
    {
        if (!(il == IsolationLevel.Snapshot || il == IsolationLevel.Unspecified))
            throw new NotSupportedException("Specified isolation level is not supported by DuckDB. ");

        return BeginTransaction();
    }
    
    #endregion
}
