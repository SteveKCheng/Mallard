using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
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

    [Fact]
    public void BigInteger()
    {
        using var dbConn = new DuckDbConnection("");

        using var ps = dbConn.CreatePreparedStatement("SELECT $1");
        Span<byte> buffer = stackalloc byte[1024];

        // Try different values
        foreach (int q in new[] { 127, 131, 163, 521, -127, -131, -163, -521 })
        {
            int p = Math.Abs(q);

            // Mersenne number 2^p - 1 
            // For example, p = 131, the number in decimal is:
            //   2722258935367507707706996859454145691647 (40 digits)

            buffer[..(p >> 3)].Fill(0xFF);
            buffer[(p >> 3)] = (byte)((1u << (1 + (p & 7))) - 1);

            int len = (p >> 3) + 1;
            buffer[len..].Clear();

            var magnitude = new BigInteger(buffer[0..len], isUnsigned: true, isBigEndian: false);
            var value = (q >= 0) ? magnitude : -magnitude;

            ps.BindParameter(1, value);

            object? valueOut = ps.ExecuteScalar();
            Assert.IsType<BigInteger>(valueOut);
            Assert.StrictEqual(value, (BigInteger)valueOut);
        }
    }
}
