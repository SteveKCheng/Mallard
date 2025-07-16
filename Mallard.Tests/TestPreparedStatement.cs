using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.Tests;

public class TestPreparedStatement(DatabaseFixture fixture) : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _fixture = fixture;
    private DuckDbConnection DbConnection => _fixture.DbConnection;

    [Fact]
    public void TestPreparedStatementCreation()
    {
        const int limitRows = 10;
        var paramName = "mktSegment";
        using var ps = DbConnection.CreatePreparedStatement($"SELECT * FROM customer WHERE c_mktsegment = ${paramName} LIMIT {limitRows}");

        Assert.Equal(paramName, ps.GetParameterName(1));
        Assert.Equal(DuckDbBasicType.VarChar, ps.GetParameterBasicType(1));
        Assert.Equal(1, ps.ParameterCount);
        Assert.Equal(1, ps.GetParameterIndexForName(paramName));

        ps.BindParameter(1, "AUTOMOBILE");

        using var dbResult = ps.Execute();
        Assert.Equal(limitRows, dbResult.DestructiveGetNumberOfResults());
    }
}
