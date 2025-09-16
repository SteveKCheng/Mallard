using System;
using Xunit;
using TUnit.Core;

namespace Mallard.Tests;

[ClassDataSource<DatabaseFixture>(Shared = SharedType.PerTestSession)]
public class TestPreparedStatement(DatabaseFixture fixture)
{
    private readonly DatabaseFixture _fixture = fixture;
    private DuckDbConnection DbConnection => _fixture.ConnectionWithTpchData;

    [Test]
    public void TestPreparedStatementCreation()
    {
        const int limitRows = 10;
        var paramName = "mktSegment";
        using var ps = DbConnection.PrepareStatement($"SELECT * FROM customer WHERE c_mktsegment = ${paramName} LIMIT {limitRows}");

        Assert.Equal(paramName, ps.GetParameterName(1));
        Assert.Equal(DuckDbValueKind.VarChar, ps.GetParameterValueKind(1));
        Assert.Equal(1, ps.ParameterCount);
        Assert.Equal(1, ps.GetParameterIndexForName(paramName));

        ps.BindParameter(1, "AUTOMOBILE");

        using var dbResult = ps.Execute();
        Assert.Equal(limitRows, dbResult.DestructiveGetNumberOfResults());
    }

    [Test]
    public void DecimalParameter()
    {
        const int limitRows = 50;
        using var ps = DbConnection.PrepareStatement($"SELECT c_name, c_acctbal FROM customer WHERE ABS(c_acctbal) >= $1 LIMIT {limitRows}");

        Assert.Equal("1", ps.GetParameterName(1));
        
        ps.BindParameter(1, 5182.05M);  // decimal literal
        
        using var dbResult = ps.Execute();
        Assert.Equal(limitRows, dbResult.DestructiveGetNumberOfResults());
    }

    [Test]
    public void NamedParameters()
    {
        using var connection = new DuckDbConnection("");

        using var ps = connection.PrepareStatement("SELECT $a // ($b // $c)");
        
        ps.BindParameter("c", 10);
        ps.BindParameter("b", 50);
        ps.BindParameter("a", 800);

        var answer = ps.ExecuteValue<int>();
        Assert.Equal(800 / (50 / 10), answer);
    }
}
