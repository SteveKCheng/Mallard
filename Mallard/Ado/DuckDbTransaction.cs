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
public readonly struct DuckDbTransaction : IDbTransaction
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
    private readonly int _version;

    /// <summary>
    /// The isolation level of the database transaction.
    /// </summary>
    /// <remarks>
    /// DuckDB only supports snapshot-level isolation (<see cref="IsolationLevel.Snapshot" />
    /// and that is what this property always reports.
    /// </remarks>
    public IsolationLevel IsolationLevel => IsolationLevel.Snapshot;

    internal DuckDbTransaction(DuckDbConnection connection)
    {
        _version = connection.BeginTransactionInternal();
        Connection = connection;
    }

    /// <inheritdoc cref="IDbTransaction.Commit" />
    public void Commit() => Connection.CommitTransactionInternal(_version);

    /// <inheritdoc cref="IDbTransaction.Rollback" />
    public void Rollback() => Connection.RollbackTransactionInternal(_version, isDisposing: false);

    /// <summary>
    /// Disposes of the transaction, equivalent to rolling it back if 
    /// it has not been committed or rolled back already.
    /// </summary>
    public void Dispose() => Connection.RollbackTransactionInternal(_version, isDisposing: true);
}

public sealed partial class DuckDbConnection
{
    #region Transactions

    /// <summary>
    /// Begin a transaction on the current connection.
    /// </summary>
    internal int BeginTransactionInternal()
    {
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
            ExecuteCommand("BEGIN TRANSACTION");
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
        VerifyTransaction(version);
        try
        {
            ExecuteCommand("COMMIT");
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
        if (!isDisposing)
            VerifyTransaction(version);
        else if (_transactionVersion == 0 || _transactionVersion != version)
            return;
        
        try
        {
            ExecuteCommand("ROLLBACK");
        }
        finally
        {
            _transactionVersion = 0;
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
    /// </remarks>
    private int _transactionVersion;

    /// <summary>
    /// Verify the version of a transaction matches the current state of this connection. 
    /// </summary>
    private void VerifyTransaction(int version)
    {
        if (version != _transactionVersion)
            throw new InvalidOperationException("Cannot commit or roll back a transaction that is no longer active. ");
    }
    
    /// <summary>
    /// Begin a transaction on the current connection.
    /// </summary>
    /// <returns>
    /// A "holder" object that is used to either commit the transaction or
    /// tp roll it back.  Put it in a <c>using</c> block in C#.
    /// </returns>
    public DuckDbTransaction BeginTransaction() => new DuckDbTransaction(this);

    #endregion
}
