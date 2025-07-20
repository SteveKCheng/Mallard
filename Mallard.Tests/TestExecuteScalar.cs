using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.Tests;

public class TestExecuteScalar
{
    [Fact]
    public void Decimal()
    {
        using var dbConn = new DuckDbConnection("");

        // This number cannot be represented exactly in floating-point
        decimal v = 0.1M;

        var ps = dbConn.CreatePreparedStatement("SELECT $1");
        ps.BindParameter(1, v);
        object? v_out = ps.ExecuteScalar();

        Assert.IsType<Decimal>(v_out);
        Assert.StrictEqual(v, (decimal)v_out);
    }
}
