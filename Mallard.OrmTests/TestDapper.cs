using TUnit.Core;
using Xunit;
using Dapper;

namespace Mallard.Tests.Orm;

internal class NorthwindCategory
{
    public int CategoryId { get; init; }
    
    public string? CategoryName { get; init; }

    public string? Description { get; init; }
    
    public byte[]? Picture { get; init; }
}


[ClassDataSource<DatabaseFixture>(Shared = SharedType.PerTestSession)]
public class TestDapper(DatabaseFixture fixture)
{
    private DatabaseFixture _fixture = fixture;
    
    [Test]
    public void DapperQuery()
    {
        var connection = _fixture.ConnectionWithNorthwind;
        var categories = connection.Query<NorthwindCategory>("SELECT * FROM Categories");
        foreach (var c in categories)
        {
            Assert.NotNull(c.CategoryName);
            Assert.NotNull(c.Description);
            Assert.NotNull(c.Picture);
        }
    }
}
