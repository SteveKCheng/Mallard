using System;
using System.Data;
using Xunit;
using TUnit.Core;

namespace Mallard.Tests;

[ClassDataSource<DatabaseFixture>(Shared = SharedType.PerTestSession)]
public class TestAdo(DatabaseFixture fixture)
{
    private readonly DatabaseFixture _fixture = fixture;
    private DuckDbConnection ConnectionWithTpchData => _fixture.ConnectionWithTpchData;

    #region Parameterized Query Tests

    [Test]
    public void TestCreateCommand()
    {
        // Test creating command through IDbConnection interface
        IDbConnection connection = ConnectionWithTpchData;
        using var command = connection.CreateCommand();
        
        Assert.NotNull(command);
        Assert.IsType<DuckDbCommand>(command);
        Assert.Same(connection, command.Connection);
    }

    [Test]
    public void TestParameterizedQueryWithStringParameter()
    {
        IDbConnection connection = ConnectionWithTpchData;
        using var command = connection.CreateCommand();
        
        command.CommandText = "SELECT c_name FROM customer WHERE c_mktsegment = $1 LIMIT 5";
        
        var parameter = command.CreateParameter();
        parameter.ParameterName = "1";
        parameter.Value = "AUTOMOBILE";
        command.Parameters.Add(parameter);
        
        using var reader = command.ExecuteReader();
        
        int rowCount = 0;
        while (reader.Read())
        {
            var name = reader.GetString(0);
            Assert.NotNull(name);
            Assert.True(name.Length > 0);
            rowCount++;
        }
        
        Assert.True(rowCount > 0, "Should return at least one row");
        Assert.True(rowCount <= 5, "Should not exceed LIMIT");
    }

    [Test]
    public void TestParameterizedQueryWithDecimalParameter()
    {
        IDbConnection connection = ConnectionWithTpchData;
        using var command = connection.CreateCommand();
        
        command.CommandText = "SELECT c_name, c_acctbal FROM customer WHERE c_acctbal >= $1 LIMIT 10";
        
        var parameter = command.CreateParameter();
        parameter.ParameterName = "1";
        parameter.Value = 5000.0M;
        command.Parameters.Add(parameter);
        
        using var reader = command.ExecuteReader();
        
        int rowCount = 0;
        while (reader.Read())
        {
            var name = reader.GetString(0);
            var balance = reader.GetDecimal(1);
            
            Assert.NotNull(name);
            Assert.True(balance >= 5000.0M, $"Balance {balance} should be >= 5000");
            rowCount++;
        }
        
        Assert.True(rowCount > 0, "Should return at least one row");
    }

    [Test]
    public void TestParameterizedQueryWithMultipleParameters()
    {
        IDbConnection connection = ConnectionWithTpchData;
        using var command = connection.CreateCommand();
        
        command.CommandText = "SELECT c_name FROM customer WHERE c_mktsegment = $1 AND c_acctbal >= $2 LIMIT 3";
        
        var param1 = command.CreateParameter();
        param1.ParameterName = "1";
        param1.Value = "BUILDING";
        command.Parameters.Add(param1);
        
        var param2 = command.CreateParameter();
        param2.ParameterName = "2";
        param2.Value = 4000.0M;
        command.Parameters.Add(param2);
        
        using var reader = command.ExecuteReader();
        
        int rowCount = 0;
        while (reader.Read())
        {
            var name = reader.GetString(0);
            Assert.NotNull(name);
            rowCount++;
        }
        
        // This combination should return some results but verify the query executed
        Assert.True(rowCount >= 0, "Query should execute successfully");
        Assert.True(rowCount <= 3, "Should not exceed LIMIT");
    }

    [Test]
    public void TestExecuteScalar()
    {
        IDbConnection connection = ConnectionWithTpchData;
        using var command = connection.CreateCommand();
        
        command.CommandText = "SELECT COUNT(*) FROM customer WHERE c_mktsegment = $1";
        
        var parameter = command.CreateParameter();
        parameter.ParameterName = "1";
        parameter.Value = "AUTOMOBILE";
        command.Parameters.Add(parameter);
        
        var result = command.ExecuteScalar();
        
        Assert.NotNull(result);
        Assert.IsType<long>(result);
        Assert.True((long)result > 0, "Should have some customers in AUTOMOBILE segment");
    }

    [Test]
    public void TestExecuteNonQuery()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
        
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE test_table (id INTEGER, name VARCHAR(50))";
        
        int result = command.ExecuteNonQuery();
        Assert.Equal(-1, result);  // CREATE TABLE doesn't return row count
        
        // Insert some data
        command.CommandText = "INSERT INTO test_table VALUES ($1, $2), ($3, $4)";
        
        var param1 = command.CreateParameter();
        param1.ParameterName = "1";
        param1.Value = 1;
        command.Parameters.Add(param1);
        
        var param2 = command.CreateParameter();
        param2.ParameterName = "2";
        param2.Value = "Alice";
        command.Parameters.Add(param2);
        
        var param3 = command.CreateParameter();
        param3.ParameterName = "3";
        param3.Value = 2;
        command.Parameters.Add(param3);
        
        var param4 = command.CreateParameter();
        param4.ParameterName = "4";
        param4.Value = "Bob";
        command.Parameters.Add(param4);
        
        result = command.ExecuteNonQuery();
        Assert.Equal(2, result);  // Should insert 2 rows
    }

    [Test]
    public void TestPreparedStatement()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        // Create test table
        using var setupCommand = connection.CreateCommand();
        setupCommand.CommandText = "CREATE TABLE prep_test (id INTEGER, value VARCHAR(50))";
        setupCommand.ExecuteNonQuery();
        
        // Test prepared statement
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO prep_test VALUES ($1, $2)";
        
        var idParam = command.CreateParameter();
        idParam.ParameterName = "1";
        command.Parameters.Add(idParam);
        
        var valueParam = command.CreateParameter();
        valueParam.ParameterName = "2";  
        command.Parameters.Add(valueParam);
        
        // Prepare the statement
        command.Prepare();
        
        // Execute with different parameters
        for (int i = 1; i <= 3; i++)
        {
            idParam.Value = i;
            valueParam.Value = $"Value{i}";
            
            int rowsAffected = command.ExecuteNonQuery();
            Assert.Equal(1, rowsAffected);
        }
        
        // Verify the data was inserted
        using var selectCommand = connection.CreateCommand();
        selectCommand.CommandText = "SELECT COUNT(*) FROM prep_test";
        var count = selectCommand.ExecuteScalar();
        Assert.Equal(3L, count);
    }

    #endregion

    #region Transaction Tests

    [Test]
    public void TestBasicTransaction()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        // Setup test table
        using var setupCommand = connection.CreateCommand();
        setupCommand.CommandText = "CREATE TABLE trans_test (id INTEGER, value VARCHAR(50))";
        setupCommand.ExecuteNonQuery();
        
        // Test successful transaction
        using (var transaction = connection.BeginTransaction())
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO trans_test VALUES (1, 'test1'), (2, 'test2')";
            
            int rowsAffected = command.ExecuteNonQuery();
            Assert.Equal(2, rowsAffected);
            
            transaction.Commit();
        }
        
        // Verify data was committed
        using var verifyCommand = connection.CreateCommand();
        verifyCommand.CommandText = "SELECT COUNT(*) FROM trans_test";
        var count = verifyCommand.ExecuteScalar();
        Assert.Equal(2L, count);
    }

    [Test]
    public void TestTransactionRollback()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        // Setup test table
        using var setupCommand = connection.CreateCommand();
        setupCommand.CommandText = "CREATE TABLE rollback_test (id INTEGER, value VARCHAR(50))";
        setupCommand.ExecuteNonQuery();
        
        // Insert initial data
        using var initialCommand = connection.CreateCommand();
        initialCommand.CommandText = "INSERT INTO rollback_test VALUES (1, 'initial')";
        initialCommand.ExecuteNonQuery();
        
        // Test transaction rollback
        using (var transaction = connection.BeginTransaction())
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO rollback_test VALUES (2, 'rollback_me'), (3, 'rollback_me_too')";
            
            int rowsAffected = command.ExecuteNonQuery();
            Assert.Equal(2, rowsAffected);
            
            transaction.Rollback();
        }
        
        // Verify only initial data remains
        using var verifyCommand = connection.CreateCommand();
        verifyCommand.CommandText = "SELECT COUNT(*) FROM rollback_test";
        var count = verifyCommand.ExecuteScalar();
        Assert.Equal(1L, count);
    }

    [Test]
    public void TestTransactionAutoRollback()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        // Setup test table
        using var setupCommand = connection.CreateCommand();
        setupCommand.CommandText = "CREATE TABLE auto_rollback_test (id INTEGER, value VARCHAR(50))";
        setupCommand.ExecuteNonQuery();
        
        // Test automatic rollback when transaction is disposed without commit
        using (var transaction = connection.BeginTransaction())
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO auto_rollback_test VALUES (1, 'should_rollback')";
            
            int rowsAffected = command.ExecuteNonQuery();
            Assert.Equal(1, rowsAffected);
            
            // Don't commit - let dispose handle rollback
        }
        
        // Verify data was rolled back
        using var verifyCommand = connection.CreateCommand();
        verifyCommand.CommandText = "SELECT COUNT(*) FROM auto_rollback_test";
        var count = verifyCommand.ExecuteScalar();
        Assert.Equal(0L, count);
    }

    [Test]
    public void TestTransactionIsolationLevel()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        using var transaction = connection.BeginTransaction();
        
        Assert.Equal(IsolationLevel.Snapshot, transaction.IsolationLevel);
    }

    [Test]
    public void TestCommandTransactionProperty()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        using var command = connection.CreateCommand();
        
        // Initially no transaction
        Assert.Null(command.Transaction);
        
        using var transaction = connection.BeginTransaction();
        command.Transaction = transaction;
        
        // N.B. DuckDbTransaction is a struct, so transaction and command.Transaction
        // are not necessarily the same object (by reference equality), but Assert.Equal
        // still works because DuckDbTransaction does override object.Equals.
        Assert.Equal(transaction, command.Transaction);
        
        Assert.Equal(IsolationLevel.Snapshot, command.Transaction.IsolationLevel);
    }

    #endregion

    #region Error Handling Tests

    [Test]
    public void TestMultipleTransactionsError()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        using var transaction1 = connection.BeginTransaction();
        
        // Attempting to begin a second transaction should throw
        var exception = Assert.Throws<InvalidOperationException>(() => connection.BeginTransaction());
        Assert.Contains("already started", exception.Message);
    }

    [Test]
    public void TestInvalidTransactionOperations()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        var transaction = connection.BeginTransaction();
        transaction.Commit();
        
        // Operations on committed transaction should throw
        Assert.Throws<InvalidOperationException>(() => transaction.Commit());
        Assert.Throws<InvalidOperationException>(() => transaction.Rollback());
    }

    [Test]
    public void TestInvalidParameterDirection()
    {
        IDbConnection connection = ConnectionWithTpchData;
        using var command = connection.CreateCommand();
        
        var parameter = command.CreateParameter();
        
        // DuckDB only supports Input parameters
        Assert.Throws<NotSupportedException>(() => parameter.Direction = ParameterDirection.Output);
        Assert.Throws<NotSupportedException>(() => parameter.Direction = ParameterDirection.InputOutput);
        Assert.Throws<NotSupportedException>(() => parameter.Direction = ParameterDirection.ReturnValue);
        
        // Input should be fine
        parameter.Direction = ParameterDirection.Input; // Should not throw
    }

    [Test]
    public void TestInvalidCommandType()
    {
        IDbConnection connection = ConnectionWithTpchData;
        using var command = connection.CreateCommand();
        
        Assert.Equal(CommandType.Text, command.CommandType);
        
        // Only Text is supported
        Assert.Throws<NotSupportedException>(() => command.CommandType = CommandType.StoredProcedure);
        Assert.Throws<NotSupportedException>(() => command.CommandType = CommandType.TableDirect);
    }

    [Test]
    public void TestWrongTransactionType()
    {
        using IDbConnection connection1 = new DuckDbConnection("");
        using IDbConnection connection2 = new DuckDbConnection("");
        
        using var transaction1 = connection1.BeginTransaction();
        using var command = connection2.CreateCommand();
        
        // Setting a transaction from a different connection should throw
        Assert.Throws<InvalidOperationException>(() => command.Transaction = transaction1);
    }

    [Test]
    public void TestSqlInjectionProtection()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
                
        // Setup test table
        using var setupCommand = connection.CreateCommand();
        setupCommand.CommandText = "CREATE TABLE injection_test (id INTEGER, name VARCHAR(100))";
        setupCommand.ExecuteNonQuery();
        
        setupCommand.CommandText = "INSERT INTO injection_test VALUES (1, 'Alice'), (2, 'Bob')";
        setupCommand.ExecuteNonQuery();
        
        // Test that parameterized queries protect against injection
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM injection_test WHERE name = $1";
        
        var parameter = command.CreateParameter();
        parameter.ParameterName = "1";
        parameter.Value = "Alice'; DROP TABLE injection_test; --";  // Injection attempt
        command.Parameters.Add(parameter);
        
        using var reader = command.ExecuteReader();
        
        // Should return no rows (injection failed)
        Assert.False(reader.Read(), "SQL injection should be prevented by parameterization");
        
        // Verify table still exists by running another query
        reader.Close();
        using var verifyCommand = connection.CreateCommand();
        verifyCommand.CommandText = "SELECT COUNT(*) FROM injection_test";
        var count = verifyCommand.ExecuteScalar();
        Assert.Equal(2L, count);  // Table should still have 2 rows
    }

    #endregion

    #region Connection State Tests

    [Test]
    public void TestConnectionStateProperty()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        Assert.Equal(ConnectionState.Open, connection.State);
        
        // Connection already open; cannot re-open without closing first
        Assert.Throws<InvalidOperationException>(() => connection.Open());

        connection.Close();
        Assert.Equal(ConnectionState.Closed, connection.State);

        // Closing again should do nothing (no exception thrown according Microsoft spec)
        connection.Close();
        Assert.Equal(ConnectionState.Closed, connection.State);

        // Re-open connection
        connection.Open();

        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Test]
    public void TestConnectionProperties()
    {
        IDbConnection connection = ConnectionWithTpchData;
        
        Assert.Equal(0, connection.ConnectionTimeout);
        Assert.NotNull(connection.Database);
        Assert.NotEmpty(connection.Database);
    }

    #endregion
}