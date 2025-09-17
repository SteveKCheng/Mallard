using System;
using System.Data;
using Xunit;
using TUnit.Core;

namespace Mallard.Tests;

/// <summary>
/// Tests to verify that the correct exception types are thrown according to .NET conventions.
/// </summary>
public class TestExceptions
{
    #region Parameter Method Tests

    [Test]
    public void TestParameterMethodsThrowArgumentOutOfRangeException()
    {
        using var connection = new DuckDbConnection("");  // In-memory database
        using var statement = connection.PrepareStatement("SELECT 1");
        
        // GetParameterName should throw ArgumentOutOfRangeException for invalid index
        var exception1 = Assert.Throws<ArgumentOutOfRangeException>(() => statement.GetParameterName(99));
        Assert.Contains("index", exception1.ParamName ?? "", StringComparison.OrdinalIgnoreCase);
        
        // GetParameterValueKind should throw ArgumentOutOfRangeException for invalid index
        var exception2 = Assert.Throws<ArgumentOutOfRangeException>(() => statement.GetParameterValueKind(99));
        Assert.Contains("index", exception2.ParamName ?? "", StringComparison.OrdinalIgnoreCase);
        
        // BindParameter should throw ArgumentOutOfRangeException for invalid index
        var exception3 = Assert.Throws<ArgumentOutOfRangeException>(() => statement.BindParameter(99, "test"));
        Assert.Contains("index", exception3.ParamName ?? "", StringComparison.OrdinalIgnoreCase);
    }

    [Test]
    public void TestParameterNameLookupThrowsArgumentException()
    {
        using var connection = new DuckDbConnection("");  // In-memory database
        using var statement = connection.PrepareStatement("SELECT 1");
        
        // GetParameterIndexForName should throw ArgumentException for nonexistent parameter
        var exception = Assert.Throws<ArgumentException>(() => statement.GetParameterIndexForName("nonexistent"));
        Assert.Contains("nonexistent", exception.Message);
    }

    #endregion

    #region ADO.NET Interface Tests

    [Test] 
    public void TestDataReaderGetOrdinalThrowsIndexOutOfRangeException()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as test_column";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        // GetOrdinal should throw IndexOutOfRangeException per ADO.NET specification
        var exception = Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("nonexistent_column"));
        Assert.Contains("name", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Test]
    public void TestParameterCollectionThrowsArgumentException()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        
        // Accessing parameter by name that doesn't exist should throw ArgumentException
        var exception = Assert.Throws<ArgumentException>(() => command.Parameters["nonexistent"]);
        Assert.Contains("nonexistent", exception.Message);
    }

    #endregion

    #region Boundary Tests

    [Test]
    public void TestValidParameterIndices()
    {
        using var connection = new DuckDbConnection("");  // In-memory database
        using var statement = connection.PrepareStatement("SELECT $1, $2");
        
        // These should work without throwing
        Assert.Equal("1", statement.GetParameterName(1));
        Assert.Equal("2", statement.GetParameterName(2));

        // Parameter value kind might be Invalid until bound - just check it doesn't throw
        var valueKind = statement.GetParameterValueKind(1);
        Assert.True(Enum.IsDefined(typeof(DuckDbValueKind), valueKind));
        
        // Boundary: index 0 should throw (1-based indexing)
        Assert.Throws<ArgumentOutOfRangeException>(() => statement.GetParameterName(0));
        
        // Boundary: index 3 should throw (only 2 parameters)
        Assert.Throws<ArgumentOutOfRangeException>(() => statement.GetParameterName(3));
    }

    [Test]
    public void TestNegativeParameterIndex()
    {
        using var connection = new DuckDbConnection("");  // In-memory database
        using var statement = connection.PrepareStatement("SELECT 1");
        
        // Negative indices should throw ArgumentOutOfRangeException
        Assert.Throws<ArgumentOutOfRangeException>(() => statement.GetParameterName(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => statement.GetParameterValueKind(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => statement.BindParameter(-1, "test"));
    }

    #endregion

    #region Parameter Name Validation Tests

    [Test]
    public void TestEmptyParameterNameThrowsArgumentException()
    {
        using var connection = new DuckDbConnection("");  // In-memory database
        using var statement = connection.PrepareStatement("SELECT 1");
        
        // Empty parameter name should throw ArgumentException
        Assert.Throws<ArgumentException>(() => statement.GetParameterIndexForName(""));
        Assert.Throws<ArgumentException>(() => statement.GetParameterIndexForName(null!));
    }

    [Test]
    public void TestValidParameterNameLookup()
    {
        using var connection = new DuckDbConnection("");  // In-memory database
        using var statement = connection.PrepareStatement("SELECT $test");
        
        // Valid parameter name should work
        var index = statement.GetParameterIndexForName("test");
        Assert.Equal(1, index);
        
        // Test with clearly non-existent parameter names
        Assert.Throws<ArgumentException>(() => statement.GetParameterIndexForName("completely_nonexistent_param_name"));
        Assert.Throws<ArgumentException>(() => statement.GetParameterIndexForName("xyz123"));
    }

    #endregion

    #region Column Access Tests

    [Test]
    public void TestValidColumnAccess()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as first_col, 2 as second_col";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        // Valid column names should work
        Assert.Equal(0, reader.GetOrdinal("first_col"));
        Assert.Equal(1, reader.GetOrdinal("second_col"));
        
        // Valid column indices should work
        Assert.Equal(1, reader.GetInt32(0));
        Assert.Equal(2, reader.GetInt32(1));
    }

    [Test]
    public void TestInvalidColumnIndex()
    {
        using IDbConnection connection = new DuckDbConnection("");  // In-memory database
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as test_col";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        // Invalid column indices should throw ArgumentOutOfRangeException
        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetInt32(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetInt32(1)); // Only 1 column (index 0)
    }

    #endregion

    #region DuckDbDataReader Exception Tests

    [Test]
    public void TestGetBytesThrowsArgumentNullException()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test data' as blob_col";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        // GetBytes should throw ArgumentNullException when buffer is null
        var exception = Assert.Throws<ArgumentNullException>(() => reader.GetBytes(0, 0, null!, 0, 10));
        Assert.Equal("buffer", exception.ParamName);
    }

    [Test]
    public void TestGetCharsThrowsArgumentNullException()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test data' as text_col";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        // GetChars should throw ArgumentNullException when buffer is null
        var exception = Assert.Throws<ArgumentNullException>(() => reader.GetChars(0, 0, null!, 0, 10));
        Assert.Equal("buffer", exception.ParamName);
    }

    [Test]
    public void TestGetBytesThrowsArgumentOutOfRangeException()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test data' as blob_col";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        var buffer = new byte[10];
        // GetBytes should throw ArgumentOutOfRangeException when length exceeds buffer capacity
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetBytes(0, 0, buffer, 5, 10));
        Assert.Equal("length", exception.ParamName);
    }

    [Test]
    public void TestGetCharsThrowsArgumentOutOfRangeException()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'test data' as text_col";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        var buffer = new char[10];
        // GetChars should throw ArgumentOutOfRangeException when length exceeds buffer capacity
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetChars(0, 0, buffer, 5, 10));
        Assert.Equal("length", exception.ParamName);
    }

    [Test]
    public void TestFieldAccessThrowsArgumentOutOfRangeException()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as col1, 2 as col2";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        // Negative column index should throw ArgumentOutOfRangeException
        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetFieldType(-1));
        
        // Column index >= FieldCount should throw ArgumentOutOfRangeException
        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetFieldType(2)); // Only 2 columns (0, 1)
    }

    [Test]
    public void TestObjectDisposedExceptionWhenReaderClosed()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as test_col";
        var reader = command.ExecuteReader();
        
        // Close the reader
        reader.Close();
        
        // Subsequent Read() calls should throw ObjectDisposedException
        var exception = Assert.Throws<ObjectDisposedException>(() => reader.Read());
        Assert.Contains("DuckDbDataReader", exception.ObjectName ?? "");
    }

    [Test]
    public void TestInvalidOperationExceptionWhenNoActiveChunk()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 as test_col";
        using var reader = command.ExecuteReader();
        
        // Before calling Read(), GetDelegateReader should throw InvalidOperationException
        var exception = Assert.Throws<InvalidOperationException>(() => reader.GetInt32(0));
        Assert.Contains("no more chunks", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Test]
    public void TestReaderExceptionScenarios()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        // Test with empty result set
        using var command1 = connection.CreateCommand();
        command1.CommandText = "SELECT 1 WHERE FALSE";  // Returns no rows
        using var reader1 = command1.ExecuteReader();
        
        // Should be able to call Read() without exception (returns false)
        Assert.False(reader1.Read());
        
        // But accessing field values should throw InvalidOperationException
        Assert.Throws<InvalidOperationException>(() => reader1.GetInt32(0));
        
        // Test multiple column scenario
        using var command2 = connection.CreateCommand();
        command2.CommandText = "SELECT 1 as a, 'test' as b, CAST(3.14 AS DOUBLE) as c";
        using var reader2 = command2.ExecuteReader();
        reader2.Read();
        
        // Valid access should work
        Assert.Equal(1, reader2.GetInt32(0));
        Assert.Equal("test", reader2.GetString(1));
        Assert.Equal(3.14, reader2.GetDouble(2), precision: 2);
        
        // Invalid column indices should throw
        Assert.Throws<ArgumentOutOfRangeException>(() => reader2.GetInt32(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => reader2.GetInt32(3));
    }

    [Test]
    public void TestDataReaderBufferBoundaryConditions()
    {
        using IDbConnection connection = new DuckDbConnection("");
        
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'Hello World' as text_data";
        using var reader = command.ExecuteReader();
        reader.Read();
        
        // Test GetBytes with exact buffer size
        var buffer = new byte[11];
        var bytesRead = reader.GetBytes(0, 0, buffer, 0, 11);
        Assert.True(bytesRead >= 0);
        
        // Test GetBytes with buffer too small (should not throw, just return partial data)
        var smallBuffer = new byte[5];
        var partialBytesRead = reader.GetBytes(0, 0, smallBuffer, 0, 5);
        Assert.True(partialBytesRead >= 0);
        
        // Test GetChars with valid buffer
        var charBuffer = new char[11];
        var charsRead = reader.GetChars(0, 0, charBuffer, 0, 11);
        Assert.True(charsRead >= 0);
    }

    #endregion
}
