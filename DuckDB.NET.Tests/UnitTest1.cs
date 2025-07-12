using System;

namespace DuckDB.Tests;

public class UnitTest1
{
    [Fact]
    public void Test1()
    {
        using var dbConn = new DuckDbConnection(@"test.db");
        return;
    }

}
