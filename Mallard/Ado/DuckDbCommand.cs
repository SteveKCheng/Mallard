using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

public sealed class DuckDbCommand : IDbCommand
{
    private readonly DuckDbConnection _connection;
    private DuckDbStatement? _statement;
    
    public void Dispose()
    {
        throw new System.NotImplementedException();
    }

    public void Cancel()
    {
        throw new System.NotImplementedException();
    }

    public IDbDataParameter CreateParameter()
    {
        throw new System.NotImplementedException();
    }

    private void BindParameters()
    {
        
    }

    public int ExecuteNonQuery()
    {
        // Re-bind parameters
        
        
        throw new System.NotImplementedException();
    }

    public IDataReader ExecuteReader()
    {
        // FIXME
        return _statement!.ExecuteReader();
    }

    public IDataReader ExecuteReader(CommandBehavior behavior)
    {
        throw new System.NotImplementedException();
    }

    public object? ExecuteScalar()
    {
        throw new System.NotImplementedException();
    }

    public void Prepare()
    {
        _statement = _connection.CreatePreparedStatement(CommandText);
    }

    [AllowNull] public string CommandText { get; set; }
    public int CommandTimeout { get; set; }
    public CommandType CommandType { get; set; }
    public IDbConnection? Connection { get; set; }

    public IDataParameterCollection Parameters { get; } = new DuckDbParameterCollection();
    
    public IDbTransaction? Transaction { get; set; }
    public UpdateRowSource UpdatedRowSource { get; set; }

    internal DuckDbCommand(DuckDbConnection connection)
    {
        _connection = connection;
    }
}
