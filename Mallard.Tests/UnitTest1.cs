using System;
using System.IO;

namespace Mallard.Tests;

public class UnitTest1(DatabaseFixture fixture) : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _fixture = fixture;

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
                var dates = reader.GetColumnRaw<DuckDbDate>(0);
                var closes = reader.GetColumnRaw<double>(1);
                var sma = reader.GetColumnRaw<double>(2);
                var volume = reader.GetColumnRaw<int>(3);

                Assert.Equal(reader.Length, closes.AsSpan().Length);
                Assert.Equal(reader.Length, volume.AsSpan().Length);

                var datesSpan = dates.AsSpan();
                var closesSpan = closes.AsSpan();
                var smaSpan = sma.AsSpan();

                // Check "Close" values are within 20% of "Sma" values,
                // as evidence that all data are being passed and interpreted correctly
                for (int i = 0; i < reader.Length; ++i)
                    Assert.InRange(closesSpan[i], smaSpan[i] * 0.80, smaSpan[i] * 1.20);

                // Check date values are in ascending order.
                for (int i = 1; i < reader.Length; ++i)
                    Assert.True(datesSpan[i].Days >= datesSpan[i-1].Days, $"Dates are not in ascending order");

                return reader.Length;
            }, out var numRows);

            totalRows += numRows;
        } while (hasChunk);

        Assert.InRange(numChunks, 1, 10);
        Assert.InRange(totalRows, 1000, 2000);
    }

    [Fact]
    public void Test3()
    {
        var dbConn = _fixture.DbConnection;

        using var dbResult = dbConn.Execute(@"
            SELECT DISTINCT c_mktsegment FROM customer ORDER BY c_mktsegment ASC");

        bool hasChunk;
        int totalRows = 0;
        int numChunks = -1;
        string? prevString = null;

        do
        {
            numChunks++;

            hasChunk = dbResult.ProcessNextChunk(false, (in DuckDbChunkReader reader, bool _) =>
            {
                var strings = reader.GetColumn<string>(0);

                for (int i = 0; i < reader.Length; ++i)
                {
                    var thisString = strings.GetItem(i);
                    Assert.NotNull(thisString);

                    Assert.True(IsAsciiString(thisString),
                                $"String contains non-ASCII characters; it may be corrupt");
                    Assert.True(prevString == null || string.CompareOrdinal(prevString, thisString) < 0,
                                $"Strings are not in ascending order");
                    prevString = thisString;
                }

                return reader.Length;
            }, out var numRows);

            totalRows += numRows;
        } while (hasChunk);
    }

    private static bool IsAsciiString(string s)
        => s.AsSpan().ContainsAnyExceptInRange('\u0020', '\u007E') == false;
}
