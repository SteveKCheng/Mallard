using System;
using System.Data;
using System.Data.Common;

namespace Mallard;

public sealed partial class DuckDbConnection : IDbConnection
{
    private string? _connectionString;
    private bool _connectionStringChanged;

    string? IDbConnection.ConnectionString
    {
        get
        {
            if (_connectionString != null)
                return _connectionString;
            
            var builder = new DbConnectionStringBuilder(useOdbcRules: true);
            builder.Add("path", _database.Path);
            if (!_database.Options.IsDefault)
            {
                foreach (var (key, value) in _database.Options)
                    builder.Add(key, value);
            }

            _connectionString = builder.ConnectionString;
            return _connectionString;
        }
        set
        {
            _connectionString = value;
            _connectionStringChanged = true;
        }
    }

    int IDbConnection.ConnectionTimeout => throw new NotImplementedException();

    string IDbConnection.Database => throw new NotImplementedException();

    ConnectionState IDbConnection.State => throw new NotImplementedException();

    IDbTransaction IDbConnection.BeginTransaction() => BeginTransaction();

    IDbTransaction IDbConnection.BeginTransaction(IsolationLevel il)
    {
        if (!(il == IsolationLevel.Snapshot || il == IsolationLevel.Unspecified))
            throw new NotSupportedException("Specified isolation level is not supported by DuckDB");

        return BeginTransaction();
    }

    void IDbConnection.ChangeDatabase(string databaseName)
    {
        throw new NotImplementedException();
    }

    void IDbConnection.Close()
    {
        Dispose();
    }

    IDbCommand IDbConnection.CreateCommand()
    {
        throw new NotImplementedException();
    }

    void IDbConnection.Open()
    {
        throw new NotImplementedException();
    }
}
