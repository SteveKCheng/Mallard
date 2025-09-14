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

    /// <inheritdoc cref="IDbTransaction.Commit" />
    public void Commit() => Connection.CommitTransactionInternal(Version);

    /// <inheritdoc cref="IDbTransaction.Rollback" />
    public void Rollback() => Connection.RollbackTransactionInternal(Version, isDisposing: false);

    /// <summary>
    /// Disposes of the transaction, equivalent to rolling it back if 
    /// it has not been committed or rolled back already.
    /// </summary>
    public void Dispose() => Connection.RollbackTransactionInternal(Version, isDisposing: true);

    /// <summary>
    /// Whether this instance and the other instance refers to the same transaction
    /// on the same database. 
    /// </summary>
    public bool Equals(DuckDbTransaction other) 
        => Connection == other.Connection && Version == other.Version;

    /// <inheritdoc cref="object.Equals" />
    public override bool Equals(object? obj)
        => obj is DuckDbTransaction other && Equals(other);
    
    /// <inheritdoc cref="object.GetHashCode" />
    public override int GetHashCode()
        => HashCode.Combine(Connection.GetHashCode(), Version);
}

public sealed partial class DuckDbConnection
{
    #region Transactions

    /// <summary>
    /// Begin a transaction on the current connection.
    /// </summary>
    private int BeginTransactionInternal()
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
            ExecuteCommand(in scope, "BEGIN TRANSACTION");
        }
        catch
        {
            _transactionVersion = 0;
            throw;
        }

        return newVersion;
    }

    /// <summary>
    /// Commit a transaction that was begun by <see cref="BeginTransaction" />.
    /// </summary>
    internal void CommitTransactionInternal(int version)
    {
        using var scope = _refCount.EnterScope(this);
        
        VerifyTransaction(in scope, version);
        try
        {
            ExecuteCommand(in scope, "COMMIT");
        }
        finally
        {
            _transactionVersion = 0;
        }
    }

    /// <summary>
    /// Roll back a transaction that was begun by <see cref="BeginTransaction" />.
    /// </summary>
    internal void RollbackTransactionInternal(int version, bool isDisposing)
    {
        try
        {
            using var scope = _refCount.EnterScope(this);

            if (!isDisposing)
                VerifyTransaction(in scope, version);
            
            // It is normal for DuckDbTransaction to be disposed after it has
            // been committed or rolled back.  Then disposal should do nothing.  
            else if (_transactionVersion == 0 || _transactionVersion != version)
                return;

            try
            {
                ExecuteCommand(in scope, "ROLLBACK");
            }
            finally
            {
                _transactionVersion = 0;
            }
        }
        catch (ObjectDisposedException)
        {
            // If the database connection was closed first, presumably roll-back has
            // already occurred (because the changes were not already committed),
            // so it should be safe to silently ignore the error.
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
    /// </remarks>
    private int _transactionVersion;

    /// <summary>
    /// Verify the version of a transaction matches the current state of this connection. 
    /// </summary>
    private void VerifyTransaction(ref readonly HandleRefCount.Scope _, int version)
    {
        if (version != _transactionVersion)
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
    /// tp roll it back.  Put it in a <c>using</c> block in C#.
    /// </returns>
    public DuckDbTransaction BeginTransaction()
        => new DuckDbTransaction(this, BeginTransactionInternal());

    #endregion
}
