using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// ADO.NET-compatible command object to execute a query/statement against a DuckDB database.
/// </summary>
/// <remarks>
/// An instance of this class should only be used from one thread at a time.
/// While multi-thread access will not cause memory/.NET runtime corruption,
/// the object may misbehave or throw exceptions due to inconsistent internal state. 
/// </remarks>
public sealed class DuckDbCommand : IDbCommand
{
    #region Command text

    /// <summary>
    /// The SQL query or statement(s) to execute.
    /// </summary>
    /// <value>
    /// The query or statement(s) in DuckDB's SQL dialect.
    /// Multiple statements may be separated/terminated by semicolons; the results returned
    /// are always from the last statement.
    /// </value>
    /// <remarks>
    /// The default value is the empty string.  This property must be changed
    /// to get a valid command.  Setting this property will invalidate any preparation
    /// for the previously-set statement(s).
    /// </remarks>
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

    /// <summary>
    /// Attempt to cancel the executing query or statement (invoked by a different thread).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method is supposed to implement <see cref="IDbCommand.Cancel" />,
    /// although its semantics are subtly different from the specification.
    /// <see cref="IDbCommand.Cancel" /> is supposed to cancel
    /// the specific command represented by its receiver.  However, DuckDB's native API does not
    /// allow cancelling specific commands, but only the currently executing command
    /// on the database connection as a whole; see <see cref="DuckDbConnection.Interrupt" />.
    /// This method can only be implemented in the same way.
    /// That is, this method might cancel the wrong command, that is not the method's
    /// receiver argument. 
    /// </para> 
    /// <para>
    /// Since the same DuckDB connection cannot run multiple queries at the same time from
    /// different threads (execution is always serialized), the divergence of what this
    /// method does from the specification means little in practice: there should be only one
    /// "active" command per connection anyway.
    /// </para>
    /// </remarks>
    /// <exception cref="ObjectDisposedException">
    /// The connection has already been disposed (closed).
    /// </exception>
    public void Cancel()
    {
        Connection.Interrupt();
    }
    
    private DuckDbStatement GetBoundStatement()
    {
        var statement = GetPreparedStatement();

        for (int i = 0; i < Parameters.Count; ++i)
        {
            var p = Parameters[i];
            var n = p.ParameterName;
            var j = string.IsNullOrEmpty(n) ? i + 1 : statement.GetParameterIndexForName(n);
            statement.BindParameter(j, p.Value);
        }

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
  
    /// <summary>
    /// Prepare the SQL statement for execution.
    /// </summary>
    /// <remarks>
    /// <para>
    /// In DuckDB, in a sense, all statements must be prepared whether they use parameters or not.
    /// The statement set in <see cref="CommandText" /> will be implicitly prepared even without
    /// this calling this method.
    /// </para>
    /// <para>
    /// Thus calling this method does not save any computational work.  However,
    /// it may still be useful to call it to check for errors in the SQL statement
    /// before proceeding to set values for the parameters.
    /// </para> 
    /// </remarks>
    /// <exception cref="ObjectDisposedException">
    /// The connection has already been disposed (closed).
    /// </exception>
    /// <exception cref="DuckDbException">
    /// There is an error in preparing the statement, e.g. from a syntax error.
    /// </exception> 
    public void Prepare()
    {
        GetPreparedStatement();
    }

    private DuckDbStatement GetPreparedStatement()
    {
        var statement = _statement;
        if (statement == null)
            statement = _statement = Connection.PrepareStatement(CommandText);
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
    public IDbDataParameter CreateParameter() => new DuckDbParameter();

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
            {
                _transaction = null;
                return null;
            }
                
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
