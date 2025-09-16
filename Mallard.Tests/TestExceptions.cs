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
}