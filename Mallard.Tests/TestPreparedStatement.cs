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
    public void PrepareStatement()
    {
        const int limitRows = 10;
        var paramName = "mktSegment";
        using var ps = DbConnection.PrepareStatement($"SELECT c_custkey, c_mktsegment FROM customer WHERE c_mktsegment = ${paramName} LIMIT {limitRows}");

        Assert.Equal(paramName, ps.GetParameterName(1));
        Assert.Equal(DuckDbValueKind.VarChar, ps.GetParameterValueKind(1));
        Assert.Equal(1, ps.ParameterCount);
        Assert.Equal(1, ps.GetParameterIndexForName(paramName));

        // Use same statement object to query for 2 parameter values
        foreach (var mktsegment in new[] { "AUTOMOBILE", "HOUSEHOLD" })
        {
            ps.BindParameter(1, mktsegment);

            using var dbResult = ps.Execute();
            
            Assert.Equal(limitRows, dbResult.DestructivelyCount());

            // Check all values for constrained column are as expected 
            dbResult.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
            {
                var mktSegmentCol = reader.GetColumn<string>(1);
                for (int i = 0; i < reader.Length; ++i)
                    Assert.Equal(mktsegment, mktSegmentCol.GetItem(i));
                return true; // unused
            });
        }
        
        // Ensure there is no error in executing again without changing any parameters
        using var _ = ps.Execute();
        
        // Not setting a parameter's value should be an error
        ps.ClearBindings();
        var e = Assert.Throws<DuckDbException>(() => ps.Execute());
        Assert.Equal(DuckDbErrorKind.InvalidInput, e.ErrorKind);
    }

    [Test]
    public void DecimalParameter()
    {
        const int limitRows = 50;
        using var ps = DbConnection.PrepareStatement($"SELECT c_name, c_acctbal FROM customer WHERE ABS(c_acctbal) >= $1 LIMIT {limitRows}");

        Assert.Equal("1", ps.GetParameterName(1));
        
        ps.BindParameter(1, 5182.05M);  // decimal literal
        
        using var dbResult = ps.Execute();
        Assert.Equal(limitRows, dbResult.DestructivelyCount());
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
