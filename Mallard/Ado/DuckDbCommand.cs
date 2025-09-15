using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// ADO.NET-compatible command object to execute a query/statement against a DuckDB database.
/// </summary>
public sealed class DuckDbCommand : IDbCommand
{
    #region Command text
    
    /// <inheritdoc cref="IDbCommand.CommandText" />
    [AllowNull]
    public string CommandText
    {
        get => _sql;
        set
        {
            _statement = null;  // invalidate prepared statement
            _sql = value ?? string.Empty;
        }
    }

    private string _sql = string.Empty;
    
    CommandType IDbCommand.CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
                throw new NotSupportedException("CommandType for DuckDbCommand may only be set to CommandType.Text. ");
        }
    }

    #endregion

    #region Command execution

    public void Cancel()
    {
        throw new System.NotImplementedException();
    }
    
    private DuckDbStatement GetBoundStatement()
    {
        var statement = GetPreparedStatement();
        foreach (var p in Parameters)
            statement.BindParameter(p.ParameterName, p.Value);
        return statement;
    }
    
    /// <inheritdoc cref="IDbCommand.ExecuteNonQuery" />
    public int ExecuteNonQuery()
    {
        var statement = GetBoundStatement();
        return (int)statement.ExecuteNonQuery();
    }

    public DuckDbDataReader ExecuteReader()
    {
        var statement = GetBoundStatement();
        return statement.ExecuteReader();
    }
    
    IDataReader IDbCommand.ExecuteReader() => ExecuteReader();

    public IDataReader ExecuteReader(CommandBehavior behavior)
    {
        throw new System.NotImplementedException();
    }

    /// <inheritdoc cref="IDbCommand.ExecuteScalar" />
    public object? ExecuteScalar()
    {
        var statement = GetBoundStatement();
        return statement.ExecuteScalar();
    }
    
    #endregion
    
    #region Preparing statements

    private DuckDbStatement? _statement;
  
    /// <inheritdoc cref="IDbCommand.Prepare" />
    public void Prepare()
    {
        GetPreparedStatement();
    }

    private DuckDbStatement GetPreparedStatement()
    {
        var statement = _statement;
        if (statement == null)
            statement = _statement = Connection.CreatePreparedStatement(CommandText);
        return statement;
    }
    
    #endregion
    
    #region Parameters

    /// <summary>
    /// The parameters that should be applied to a parameterized SQL query or statement.
    /// </summary>
    public DuckDbParameterCollection Parameters { get; } = new DuckDbParameterCollection();
    
    IDataParameterCollection IDbCommand.Parameters => Parameters;
    
    /// <inheritdoc cref="IDbCommand.CreateParameter" />
    public IDbDataParameter CreateParameter() => throw new NotImplementedException();

    #endregion

    #region Connection management
    
    /// <summary>
    /// The connection that this command works on.
    /// </summary>
    public DuckDbConnection Connection { get; private set; }

    IDbConnection? IDbCommand.Connection
    {
        get => Connection;
        set
        {
            if (value is not DuckDbConnection c)
            {
                throw new InvalidOperationException(
                    "The Connection property on DuckDbCommand must be set to an instance of DuckDbConnection. ");
            }
            
            _statement = null;  // invalidate prepared statement
            Connection = c;
        }
    }
    
    // Value is ignored
    int IDbCommand.CommandTimeout { get; set; }

    /// <summary>
    /// Cached boxed instance of <see cref="DuckDbTransaction" />.
    /// </summary>
    /// <remarks>
    /// This member exists solely to implement the rather useless interface property
    /// <see cref="IDbCommand.Transaction" />.  There is no interaction with
    /// <see cref="DuckDbTransaction" /> otherwise within this class.
    /// The parent connection always maintains the state of the transaction, but
    /// an object reference is cached so that the same .NET object (after boxing
    /// the <see cref="DuckDbTransaction" /> structure) can be
    /// consistently returned from the <see cref="IDbCommand.Transaction" /> property.
    /// </remarks>
    private IDbTransaction? _transaction;

    IDbTransaction? IDbCommand.Transaction
    {
        get
        {
            if (!Connection.TryGetCurrentTransaction(out var s))
                return null;
            
            var transaction = _transaction;
            if (!s.Equals(transaction))
                _transaction = transaction = (IDbTransaction)s;
                
            return transaction;
        }
        set
        {
            var newTransaction = value as DuckDbTransaction?;
            
            // Wrong type of transaction object
            if (value != null && newTransaction == null)
                throw new InvalidOperationException("A non-DuckDB transaction may not be set on DuckDbCommand. ");

            bool isMatching =
                Connection.TryGetCurrentTransaction(out var s)
                    ? newTransaction != null && s.Equals(newTransaction.Value)
                    : newTransaction == null;

            if (!isMatching)
                throw new InvalidOperationException("A non-current transaction may not be set onto DuckDbCommand. ");

            _transaction = (IDbTransaction?)newTransaction;
        }
    }
    
    #endregion
    
    #region DataSet-related
    
    /// <inheritdoc cref="IDbCommand.UpdatedRowSource" />
    public UpdateRowSource UpdatedRowSource { get; set; }
    
    #endregion

    #region Construction and destruction
    
    internal DuckDbCommand(DuckDbConnection connection)
    {
        Connection = connection;
    }
    
    /// <inheritdoc cref="IDisposable.Dispose" />
    public void Dispose()
    {
        var statement = _statement;
        _statement = null;
        statement?.Dispose();
    }

    #endregion
}
