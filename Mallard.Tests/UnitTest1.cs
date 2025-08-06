using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Linq;

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

        int totalRows = 0;
        int numChunks = 0;
        string? prevString = null;

        dbResult.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) => {
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

            numChunks++;
            totalRows += reader.Length;

            return true;    // unused return value
        });
    }

    private static bool IsAsciiString(string s)
        => s.AsSpan().ContainsAnyExceptInRange('\u0020', '\u007E') == false;

    // Test "raw" way of reading bit strings
    [Fact]
    public void BitStringRaw()
    {
        using var dbConn = new DuckDbConnection("");
        using var ps = dbConn.CreatePreparedStatement("SELECT $1::BITSTRING");

        // Same code as in TestExecutionScalar.BitString to generate a random BitArray
        Span<byte> buffer = stackalloc byte[512];
        var random = new Random(Seed: 37);
        int numBits = 1029;
        int numBytes = (numBits + 7) / 8;
        random.NextBytes(buffer[..numBytes]);
        buffer[numBytes - 1] &= (byte)(uint.MaxValue >> (32 - (numBits & 7)));

        var bitArray = new BitArray(buffer[..numBytes].ToArray());
        var bitStringAsString = TestExecuteScalar.CreateStringFromBitArray(bitArray, 0, numBits);
        ps.BindParameter(1, bitStringAsString);

        using var dbResult = ps.Execute();
        dbResult.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            var column = reader.GetColumnRaw<DuckDbBitString>(0);
            var bitString = column.GetItem(0);

            Span<byte> segment = stackalloc byte[512];
            foreach ((int offset, int length) in new[] { (0, numBits), (5, 56), (61, 64), (numBits, 0), (800, 229), (768, 256)})
            {
                int countBytes = bitString.GetSegment(segment, offset, length);
                Assert.Equal((length + 7) / 8, countBytes);

                // Check that each bit in segment matches corresponding bit in original bitArray
                for (int i = 0; i < length; ++i)
                {
                    bool bit = (segment[i / 8] & (1 << (i % 8))) != 0;
                    Assert.Equal(bitArray[i + offset], bit);
                }
            }

            return true;    // unused return value
        });
    }

    [Fact]
    public void ReadRawStruct()
    {
        using var dbConn = new DuckDbConnection("");
        using var dbResult = dbConn.Execute("SELECT struct_pack(re := 0.0::DOUBLE, im := pi()::DOUBLE) AS z");

        dbResult.ProcessNextChunk(false, (in DuckDbChunkReader reader, bool _) =>
        {
            var column = reader.GetColumnRaw<DuckDbStructRef>(0);
            Assert.Equal(2, column.ColumnInfo.ElementSize);
            Assert.Equal("re", column.GetMemberName(0));
            Assert.Equal("im", column.GetMemberName(1));

            var re = column.GetMemberItemsRaw<double>(0).GetItem(0);
            var im = column.GetMemberItemsRaw<double>(1).GetItem(0);

            Assert.Equal(0.0, re);
            Assert.Equal(Math.PI, im, tolerance: 1e-15);
            return true;    // unused return value
        }, out _);
    }
}
