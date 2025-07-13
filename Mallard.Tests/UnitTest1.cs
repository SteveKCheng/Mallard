using System;
using System.IO;

namespace Mallard.Tests;

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
            numChunks++;
            hasChunk = dbResult.ProcessNextChunk(false, (in DuckDbChunkReader reader, bool _) =>
            {
                var dates = reader.GetColumn(0);
                var closes = reader.GetColumn(1);
                var sma = reader.GetColumn(2);
                var volume = reader.GetColumn(3);

                Assert.Equal(reader.Length, closes.AsSpan<double>().Length);
                Assert.Equal(reader.Length, volume.AsSpan<int>().Length);

                var closesVec = closes.AsSpan<double>();
                var smaVec = sma.AsSpan<double>();

                // Check "Close" values are within 20% of "Sma" values,
                // as evidence that all data are being passed and interpreted correctly
                for (int i = 0; i < reader.Length; ++i)
                    Assert.InRange(closesVec[i], smaVec[i] * 0.80, smaVec[i] * 1.20);

                return reader.Length;
            }, out var numRows);

            totalRows += numRows;
        } while (hasChunk);

        Assert.InRange(numChunks, 1, 10);
        Assert.InRange(totalRows, 1000, 2000);
    }
}
