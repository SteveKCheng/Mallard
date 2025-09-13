using System;
using System.Data;

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
public struct DuckDbTransaction : IDbTransaction
{
    public IDbConnection? Connection => throw new NotImplementedException();

    /// <summary>
    /// The isolation level of the database transaction.
    /// </summary>
    /// <remarks>
    /// DuckDB only supports snapshot-level isolation (<see cref="IsolationLevel.Snapshot" />
    /// and that is what this property always reports.
    /// </remarks>
    public readonly IsolationLevel IsolationLevel => IsolationLevel.Snapshot;

    private DuckDbConnection? _connection;

    internal DuckDbTransaction(DuckDbConnection connection)
    {
        connection.BeginTransactionInternal();
        _connection = connection;
    }

    /// <inheritdoc cref="IDbTransaction.Commit" />
    public void Commit()
    {
        ObjectDisposedException.ThrowIf(_connection == null, this);
        var connection = _connection;
        _connection = null;
        connection.CommitTransactionInternal();
    }

    /// <inheritdoc cref="IDbTransaction.Rollback" />
    public void Rollback()
    {
        ObjectDisposedException.ThrowIf(_connection == null, this);
        var connection = _connection;
        _connection = null;
        connection.RollbackTransactionInternal();
    }

    /// <summary>
    /// Disposes of the transaction, equivalent to rolling it back if 
    /// it has not been committed or rolled back already.
    /// </summary>
    public void Dispose()
    {
        if (_connection != null)
            Rollback();
    }
}
