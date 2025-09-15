using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Mallard.C_API;

namespace Mallard;

public sealed partial class DuckDbConnection : IDbConnection
{
    #region Connection strings
    
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
    
    /// <summary>
    /// The settings key in an ADO.NET-style connection string that is used
    /// to specify the path to the DuckDB database.
    /// </summary>
    private const string ConnectionStringPathKey = "path";
    
    [AllowNull] // This attribute appears in the IDbConnection interface:
    // "get" should not return null but "set" accepts null for this property.
    string IDbConnection.ConnectionString
    {
        get
        {
            var connectionString = _connectionString;
            if (connectionString != null)
                return connectionString;

            var database = _database;
            var builder = new DbConnectionStringBuilder(useOdbcRules: true);
            builder.Add(ConnectionStringPathKey, database.Path);
            if (!database.Options.IsDefault)
            {
                foreach (var (key, value) in database.Options)
                    builder.Add(key, value);
            }

            connectionString = builder.ConnectionString;
            
            // If some other thread changed _connectString first then return that one instead.
            return Interlocked.CompareExchange(ref _connectionString, connectionString, null) 
                   ?? connectionString;
        }
        set
        {
            lock (MutexForIDbConnection)
            {
                if (!_isSafeToResurrect)
                {
                    throw new InvalidOperationException(
                        "ConnectionString cannot be changed while the connection is still open. ");
                }
                
                _connectionString = value ?? string.Empty;
                _connectionStringChanged = true;
            }
        }
    }

    /// <summary>
    /// Parse a ADO.NET-style connection string to a database path and options
    /// for <see cref="DuckDbDatabase" />.
    /// </summary>
    /// <param name="connectionString">
    /// The ADO.NET-style connection string.
    /// </param>
    /// <param name="path">
    /// Path to the database file in DuckDB syntax.
    /// </param>
    /// <param name="options">
    /// List of key-value pairs for setting options on the DuckDB database.
    /// The keys are not necessarily in the order that they appear in the
    /// connection string.
    /// </param>
    private static void ParseConnectionString(string? connectionString, 
                                              out string path, 
                                              out ImmutableArray<KeyValuePair<string, string>> options)
    {
        path = string.Empty;

        if (connectionString == null)
        {
            options = ImmutableArray<KeyValuePair<string, string>>.Empty;
            return;
        }
        
        var builder = new DbConnectionStringBuilder(useOdbcRules: true)
        {
            ConnectionString = connectionString
        };

        var arrayBuilder = ImmutableArray.CreateBuilder<KeyValuePair<string, string>>(builder.Count);

        // DbConnectionStringBuilder is a .NET 1.0-era non-strongly-typed collection
        var e = ((IDictionary)builder).GetEnumerator();
        while (e.MoveNext())
        {
            var key = (string)e.Key;
            var value = (string?)e.Value ?? string.Empty;
            if (string.Equals(key, ConnectionStringPathKey, StringComparison.OrdinalIgnoreCase))
                path = value;
            else
                arrayBuilder.Add(new KeyValuePair<string, string>(key, value));
        }

        options = arrayBuilder.DrainToImmutable();
    }

    #endregion
    
    #region Re-opening connections

    /// <summary>
    /// Lock object to implement properties/methods of <see cref="IDbConnection" />
    /// pertaining to re-opening connections. 
    /// </summary>
    /// <remarks>
    /// Changing which database to connect to is not recommended usage in Mallard;
    /// it is only supported for compatibility with ADO.NET.  Thus this lock
    /// object is only allocated on first use.
    /// </remarks>
    [field: AllowNull]
    private Lock MutexForIDbConnection
    {
        get
        {
            var t = field;
            if (t != null)
                return t;

            t = new Lock();
            return Interlocked.CompareExchange(ref field, t, null) ?? t;
        }
    }

    void IDbConnection.Close() => Dispose();

    unsafe void IDbConnection.Open()
    {
        lock (MutexForIDbConnection)
        {
            if (!_isSafeToResurrect)
                throw new InvalidOperationException("Database is already open. ");
            
            // Only this method can resurrect this instance, and all code here
            // runs under a lock, so from this point of execution we can assume this
            // instance remains dead, until it is resurrected just before we release
            // the lock.  
            
            DuckDbDatabase database;
            _duckdb_connection* nativeConn;
            
            if (_connectionStringChanged)
            {
                // The user has set the connection string so we need to parse it 
                ParseConnectionString(_connectionString, out var path, out var options);
                nativeConn = DuckDbDatabase.Connect(path, options, out database);
            }
            else
            {
                // Connection string did not change; can re-use path and options from before
                database = _database;
                nativeConn = DuckDbDatabase.Reconnect(ref database); 
            }
           
            // Changing these variables is allowed here because:
            //   (a) DisposeImpl has finished,
            //   (b) this method is the only place where these variables are changed
            //       (outside the constructor and DisposeImpl), and
            //   (c) LockForIDbConnection is still locked;
            // so other threads are guaranteed not to touch these variables,
            // until this method resurrects the instance.
            //
            // If connecting fails, then this object remains in the disposed state,
            // and the variables are left unchanged.
            _nativeConn = nativeConn;
            _database = database;
            
            // database.Options already contains the parsed options
            _connectionStringChanged = false;

            // Resurrection should never fail with the assumptions given above
            _isSafeToResurrect = false;
            _refCount.TryResurrect(out var scope);
            scope.Dispose();
        }
    }
    
    #endregion

    #region Other connection state

    int IDbConnection.ConnectionTimeout => 0;

    ConnectionState IDbConnection.State
        => _isSafeToResurrect ? ConnectionState.Closed 
            : ConnectionState.Open;
    
    #endregion

    #region Which database is being used in SQL statements

    string IDbConnection.Database 
        => ExecuteValue<string>("SELECT current_database()") ?? string.Empty;

    void IDbConnection.ChangeDatabase(string databaseName)
    {
        ExecuteNonQuery($"USE {new SqlIdentifier(databaseName)}");
    }
    
    #endregion

    #region Commands
   
    IDbCommand IDbConnection.CreateCommand() => new DuckDbCommand(this);

    #endregion
}
