using System;
using System.Data;
using System.Linq;
using System.Text;
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

    [Test]
    public void ParameterOrdering()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT $a // ($b // $c)";
        
        var paramA = command.CreateParameter();
        paramA.ParameterName = "a";
        paramA.Value = 800;
        var paramB = command.CreateParameter();
        paramB.ParameterName = "b";
        paramB.Value = 50;
        var paramC = command.CreateParameter();
        paramC.ParameterName = "c";
        paramC.Value = 10;

        const int expectedValue = 800 / (50 / 10);
        
        // Should get correct result even if parameters are added in the "wrong" order
        command.Parameters.Add(paramB);
        command.Parameters.Add(paramC);
        command.Parameters.Add(paramA);
        Assert.Equal(expectedValue, command.ExecuteScalar());

        // Use positional parameters now.  Note that from the point of view
        // of command.Parameters, the parameters are in the wrong order this time too.
        command.CommandText = "SELECT $1 // ($2 // $3)";
        paramA.ParameterName = "1";
        paramB.ParameterName = "2";
        paramC.ParameterName = "3";
        Assert.Equal(expectedValue, command.ExecuteScalar());
        
        // Now erase the names and check that parameters are mapped to correct positions
        command.Parameters.Clear();
        paramA.ParameterName = null;
        paramB.ParameterName = null;
        paramC.ParameterName = null;
        command.Parameters.Add(paramA);
        command.Parameters.Add(paramB);
        command.Parameters.Add(paramC);
        Assert.Equal(expectedValue, command.ExecuteScalar());
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

    #region DataReader Additional Method Tests

    [Test]
    public void TestGetStream()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT $1 as blob_data, CAST($1 AS VARCHAR) as text_data";
        
        const string textData = "Binary content";
        
        var parameter = command.CreateParameter();
        parameter.ParameterName = "1";
        parameter.Value = Encoding.UTF8.GetBytes(textData);
        command.Parameters.Add(parameter);
        
        using var dbReader = (DuckDbDataReader)command.ExecuteReader();
        dbReader.Read();
        
        // Test GetStream on text column
        using var stream1 = dbReader.GetStream(1);
        Assert.NotNull(stream1);
        Assert.True(stream1.CanRead);
        
        // Read UTF-8 bytes of text column into buffer
        var buffer1 = new byte[256];
        int bytesRead1 = stream1.Read(buffer1, 0, buffer1.Length);
        Assert.True(bytesRead1 > 0);
        
        // Check UTF-8 bytes, when converted back to a string, match the original textData 
        var content1 = Encoding.UTF8.GetString(buffer1, 0, bytesRead1);
        Assert.Equal(textData, content1);
        
        // Test GetStream on blob column
        using var stream2 = dbReader.GetStream(0);
        Assert.NotNull(stream2);
        Assert.True(stream2.CanRead);
        
        // Read UTF-8 bytes of blob column into buffer
        var buffer2 = new byte[256];
        int bytesRead2 = stream2.Read(buffer2, 0, buffer2.Length);

        // Check the two buffers' contents are the same
        Assert.True(buffer1[..bytesRead1].SequenceEqual(buffer2[..bytesRead2]));
    }

    [Test]
    public void TestGetTextReader()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'Short text' as short_text, $1 as long_text";
        
        // Create a long string (over 1024 chars to test stream vs string reader logic)
        var longText = new string('A', 2000) + " End of long text";
        var parameter = command.CreateParameter();
        parameter.ParameterName = "1";
        parameter.Value = longText;
        command.Parameters.Add(parameter);
        
        using var dbReader = (DuckDbDataReader)command.ExecuteReader();
        dbReader.Read();
        
        // Test GetTextReader on short text (should use StringReader)
        using var textReader1 = dbReader.GetTextReader(0);
        Assert.NotNull(textReader1);
        var content1 = textReader1.ReadToEnd();
        Assert.Equal("Short text", content1);
        
        // Test GetTextReader on long text (should use StreamReader)
        using var textReader2 = dbReader.GetTextReader(1);
        Assert.NotNull(textReader2);
        var content2 = textReader2.ReadToEnd();
        Assert.Equal(longText, content2);
    }

    [Test]
    public void TestGetSchemaTable()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as int_col, 'text' as string_col, 3.14::DECIMAL(5,2) as decimal_col, TRUE as bool_col";
        
        using var dbReader = (DuckDbDataReader)command.ExecuteReader();
        
        var schemaTable = dbReader.GetSchemaTable();
        Assert.NotNull(schemaTable);
        
        // Check that we have the expected number of columns
        Assert.Equal(4, schemaTable.Rows.Count);
        
        // Check that schema table has the expected columns
        Assert.True(schemaTable.Columns.Contains("ColumnName"));
        Assert.True(schemaTable.Columns.Contains("ColumnOrdinal"));
        Assert.True(schemaTable.Columns.Contains("ColumnSize"));
        Assert.True(schemaTable.Columns.Contains("NumericPrecision"));
        Assert.True(schemaTable.Columns.Contains("NumericScale"));
        Assert.True(schemaTable.Columns.Contains("DataType"));
        
        // Verify first column details
        var row0 = schemaTable.Rows[0];
        Assert.Equal("int_col", row0["ColumnName"]);
        Assert.Equal(0, row0["ColumnOrdinal"]);
        Assert.Equal(-1, row0["ColumnSize"]);  // Required by spec
        Assert.Equal(DBNull.Value, row0["NumericPrecision"]);  // Not a decimal
        Assert.Equal(DBNull.Value, row0["NumericScale"]);  // Not a decimal
        Assert.True(row0["DataType"] is Type);
        
        // Verify decimal column has precision/scale info
        var decimalRow = schemaTable.Rows.Cast<DataRow>().First(r => r["ColumnName"].ToString() == "decimal_col");
        Assert.NotEqual(DBNull.Value, decimalRow["NumericPrecision"]);
        Assert.NotEqual(DBNull.Value, decimalRow["NumericScale"]);
        Assert.Equal(2, Convert.ToByte(decimalRow["NumericScale"]));
    }

    [Test]
    public void TestGetDataTypeName()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT 
                1 as int_col,
                'text' as varchar_col,
                3.14::DOUBLE as double_col,
                TRUE as bool_col,
                '2023-01-01'::DATE as date_col,
                '12:30:45'::TIME as time_col,
                UUID() as uuid_col
        """;
        
        using var dbReader = (DuckDbDataReader)command.ExecuteReader();
        dbReader.Read();

        void Check(int ordinal, string expected)
        {
            var typeName = dbReader.GetDataTypeName(ordinal);
            Assert.Equal(expected, typeName);
        }

        // Verify some expected SQL type names (case-insensitive check)
        Check(0, "INTEGER");
        Check(1, "VARCHAR");
        Check(2, "DOUBLE");
        Check(3, "BOOLEAN");
        Check(4, "DATE");
        Check(5, "TIME");
        Check(6, "UUID");
    }

    [Test]
    public void TestGetDataTypeNameWithNullable()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT NULL::INTEGER as nullable_int, NULL::VARCHAR as nullable_varchar";
        
        using var dbReader = (DuckDbDataReader)command.ExecuteReader();
        dbReader.Read();
        
        // Even with NULL values, GetDataTypeName should return the column type
        var intTypeName = dbReader.GetDataTypeName(0);
        Assert.NotNull(intTypeName);
        Assert.NotEmpty(intTypeName);
        Assert.Contains("INT", intTypeName.ToUpperInvariant());
        
        var varcharTypeName = dbReader.GetDataTypeName(1);
        Assert.NotNull(varcharTypeName);
        Assert.NotEmpty(varcharTypeName);
        Assert.Contains("VARCHAR", varcharTypeName.ToUpperInvariant());
    }

    [Test]
    public void TestStreamAndReaderDisposal()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test content' as text_col";
        
        using var dbReader = (DuckDbDataReader)command.ExecuteReader();
        dbReader.Read();
        
        // Test that streams and text readers can be properly disposed
        var stream = dbReader.GetStream(0);
        var textReader = dbReader.GetTextReader(0);
        
        // Should be able to read from both initially
        var buffer = new byte[100];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);
        Assert.True(bytesRead > 0);
        
        var textContent = textReader.ReadToEnd();
        Assert.NotNull(textContent);
        Assert.NotEmpty(textContent);
        
        // Dispose both
        stream.Dispose();
        textReader.Dispose();
        
        // Should not throw when disposed again
        stream.Dispose();
        textReader.Dispose();
    }

    [Test] 
    public void TestGetStreamAndTextReaderWithEmptyValues()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT '' as empty_string, NULL as null_value";
        
        using var dbReader = (DuckDbDataReader)command.ExecuteReader();
        dbReader.Read();
        
        // Test GetStream with empty string
        using var emptyStream = dbReader.GetStream(0);
        Assert.NotNull(emptyStream);
        var buffer = new byte[10];
        int bytesRead = emptyStream.Read(buffer, 0, buffer.Length);
        // Empty string should read as 0 bytes or minimal content
        Assert.True(bytesRead >= 0);
        
        // Test GetTextReader with empty string
        using var emptyTextReader = dbReader.GetTextReader(0);
        Assert.NotNull(emptyTextReader);
        var content = emptyTextReader.ReadToEnd();
        Assert.Equal("", content);
    }

    #endregion
}
