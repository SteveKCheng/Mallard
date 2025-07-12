using System;
using System.IO;

namespace DuckDB.Tests;

public class UnitTest1
{
    [Fact]
    public void Test1()
    {
        using var dbConn = new DuckDbConnection(@"test.db");
        return;
    }

    [Fact]
    public void Test2()
    {
        using var dbConn = new DuckDbConnection("");

        var testFilePath = Path.Join(Program.TestDataDirectory, "QQQ.parquet");
        dbConn.ExecuteNonQuery($"CREATE TEMP TABLE PriceQQQ AS SELECT * FROM read_parquet('{testFilePath}')");

        using var dbResult = dbConn.Execute(@"
            SELECT 
                Date, 
                CAST(Close AS DOUBLE) AS Close,
                AVG(Close) OVER(ORDER BY Date ASC ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS Sma,
                Volume
            FROM PriceQQQ
                ORDER BY Date ASC");

        bool hasChunk;
        int totalRows = 0;
        int numChunks = -1;
        do
        {
            hasChunk = dbResult.ProcessNextChunk(false, (in DuckDbChunkReader a, bool _) =>
            {
                var dates = a.GetColumn(0);
                var closes = a.GetColumn(1);
                var sma = a.GetColumn(2);
                var volume = a.GetColumn(3);

                Assert.Equal(a.Length, closes.AsSpan<double>().Length);
                Assert.Equal(a.Length, volume.AsSpan<int>().Length);

                var closesVec = closes.AsSpan<double>();
                var smaVec = sma.AsSpan<double>();

                // Check "Close" values are within 20% of "Sma" values,
                // as evidence that all data are being passed and interpreted correctly
                for (int i = 0; i < a.Length; ++i)
                    Assert.InRange(closesVec[i], smaVec[i] * 0.80, smaVec[i] * 1.20);

                return a.Length;
            }, out var numRows);

            totalRows += numRows;
            numChunks++;
        } while (hasChunk);

        Assert.InRange(numChunks, 1, 10);
        Assert.InRange(totalRows, 1000, 2000);
    }
}
