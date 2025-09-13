using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

public sealed partial class DuckDbConnection : IDbConnection
{
    /// <summary>
    /// Cached connection string returned by <see cref="IDbConnection.ConnectionString" />.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class, and DuckDB, do not use the .NET-/ODBC-style connection string to open 
    /// databases, yet constructing the string is relatively expensive, so it is cached
    /// on the first query (which might not happen at all).
    /// </para>
    /// <para>
    /// If the user sets the connection string, then this variables contains that
    /// setting.  Its value will then be used when <see cref="IDbConnection.Open" />
    /// is called.
    /// </para>
    /// </remarks>
    private string? _connectionString;
    
    /// <summary>
    /// Set to true if the user changed <see cref="IDbConnection.ConnectionString" />.
    /// </summary>
    /// <remarks>
    /// If true, the connection string needs to be parsed when
    /// <see cref="IDbConnection.Open" /> is called.
    /// </remarks>
    private bool _connectionStringChanged;

    [AllowNull] // This attribute appears in the IDbConnection interface:
                // "get" should not return null but "set" accepts null for this property.
    string IDbConnection.ConnectionString
    {
        get
        {
            if (_connectionString != null)
                return _connectionString;

            var database = _database;
            var builder = new DbConnectionStringBuilder(useOdbcRules: true);
            builder.Add("path", database.Path);
            if (!database.Options.IsDefault)
            {
                foreach (var (key, value) in database.Options)
                    builder.Add(key, value);
            }

            _connectionString = builder.ConnectionString;
            return _connectionString;
        }
        set
        {
            _connectionString = value ?? string.Empty;
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
