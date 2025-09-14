using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// ADO.NET-compatible command object to execute a query/statement against a DuckDB database.
/// </summary>
public sealed class DuckDbCommand : IDbCommand
{
    private readonly DuckDbConnection _connection;
    private DuckDbStatement? _statement;
    
    public void Dispose()
    {
    }

    public void Cancel()
    {
        throw new System.NotImplementedException();
    }

    public IDbDataParameter CreateParameter()
    {
        throw new System.NotImplementedException();
    }

    private void BindParameters(DuckDbStatement statement)
    {
        foreach (var p in _parameters)
            statement.BindParameter(p.ParameterName, p.Value);
    }

    public int ExecuteNonQuery()
    {
        var statement = GetPreparedStatement();
        BindParameters(statement);
        return (int)statement.ExecuteNonQuery();
    }

    public IDataReader ExecuteReader()
    {
        var statement = GetPreparedStatement();
        BindParameters(statement);
        return statement.ExecuteReader();
    }

    public IDataReader ExecuteReader(CommandBehavior behavior)
    {
        throw new System.NotImplementedException();
    }

    public object? ExecuteScalar()
    {
        var statement = GetPreparedStatement();
        BindParameters(statement);
        return statement.ExecuteScalar();
    }

    public void Prepare()
    {
        GetPreparedStatement();
    }

    private DuckDbStatement GetPreparedStatement()
    {
        var statement = _statement;
        if (statement == null)
            statement = _statement = _connection.CreatePreparedStatement(CommandText);
        return statement;
    }

    [AllowNull]
    public string CommandText
    {
        get => _sql;
        set
        {
            _sql = value ?? string.Empty;
            _statement = null;  // invalidate prepared statement
        }
    }

    private string _sql = string.Empty;
    
    int IDbCommand.CommandTimeout { get; set; }

    CommandType IDbCommand.CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
                throw new NotSupportedException("CommandType for DuckDbCommand may only be set to CommandType.Text. ");
        }
    }

    IDbConnection? IDbCommand.Connection
    {
        get => _connection;
        set => throw new System.NotImplementedException("The Connection property may not be set on DuckDbCommand. ");
    }
    
    public DuckDbConnection Connection => _connection;

    private readonly DuckDbParameterCollection _parameters = new DuckDbParameterCollection();
    
    IDataParameterCollection IDbCommand.Parameters => _parameters;

    public IDbTransaction? Transaction { get; set; }
    
    public UpdateRowSource UpdatedRowSource { get; set; }

    internal DuckDbCommand(DuckDbConnection connection)
    {
        _connection = connection;
    }
}
