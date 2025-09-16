using System;
using System.Linq;
using System.Text;
using Xunit;
using TUnit.Core;

namespace Mallard.Tests;

[ClassDataSource<DatabaseFixture>(Shared = SharedType.PerTestSession)]
public class TestRowOriented(DatabaseFixture fixture)
{
    private readonly DatabaseFixture _fixture = fixture;
    private DuckDbConnection DbConnection => _fixture.ConnectionWithTpchData;

    [Test]
    public void DataReader()
    {
        const int limitRows = 200;
        using var ps = DbConnection.PrepareStatement($"SELECT * FROM orders LIMIT {limitRows}");
        using var adoReader = ps.ExecuteReader();

        var columnNames = new string[]
        {
            "o_orderkey",       // BIGINT
            "o_custkey",        // BIGINT
            "o_orderstatus",    // VARCHAR
            "o_totalprice",     // DECIMAL(15,2)
            "o_orderdate",      // DATE
            "o_orderpriority",  // VARCHAR
            "o_clerk",          // VARCHAR
            "o_shippriority",   // INTEGER
            "o_comment"         // VARCHAR
        };

        Assert.Equal(columnNames.Length, adoReader.FieldCount);

        var adoColumnNames = Enumerable.Range(0, columnNames.Length)
                                       .Select(j => adoReader.GetName(j));
        Assert.Equal(columnNames, adoColumnNames);

        Assert.Equal(-1, adoReader.RecordsAffected);
        Assert.True(adoReader.HasRows);

        // Check results row by row
        ps.Execute().ProcessAllChunks(false, (in DuckDbChunkReader chunkReader, bool _) =>
        {
            var o_orderkey = chunkReader.GetColumn<long>(0);
            var o_custkey = chunkReader.GetColumn<long>(1);
            var o_orderstatus = chunkReader.GetColumn<string>(2);
            var o_totalprice = chunkReader.GetColumn<decimal>(3);
            var o_orderdate = chunkReader.GetColumn<DateTime>(4);
            var o_orderpriority = chunkReader.GetColumn<string>(5);
            var o_clerk = chunkReader.GetColumn<string>(6);
            var o_shippriority = chunkReader.GetColumn<int>(7);
            var o_comment = chunkReader.GetColumn<string>(8);

            for (int i = 0; i < chunkReader.Length; ++i)
            {
                Assert.True(adoReader.Read());
                Assert.Equal(o_orderkey.GetItem(i), adoReader.GetInt64(0));
                Assert.Equal(o_custkey.GetItem(i), adoReader.GetInt64(1));
                Assert.Equal(o_orderstatus.GetItem(i), adoReader.GetString(2));
                Assert.Equal(o_totalprice.GetItem(i), adoReader.GetDecimal(3));
                Assert.Equal(o_orderdate.GetItem(i), adoReader.GetDateTime(4));
                Assert.Equal(o_orderpriority.GetItem(i), adoReader.GetString(5));
                Assert.Equal(o_clerk.GetItem(i), adoReader.GetString(6));
                Assert.Equal(o_shippriority.GetItem(i), adoReader.GetInt32(7));
                Assert.Equal(o_comment.GetItem(i), adoReader.GetString(8));

                if (i % 50 == 0)
                {
                    for (int j = 0; j < adoReader.FieldCount; ++j)
                    {
                        Assert.Equal(adoReader[j], adoReader[columnNames[j]]);
                    }
                }
            }

            return true;
        });

        Assert.False(adoReader.Read());
    }

    [Test]
    public void GetChars()
    {
        using var dbConn = new DuckDbConnection("");
        using var ps = DbConnection.PrepareStatement($"SELECT $1::STRING");

        // Use Unicode characters with various lengths in UTF-8 and UTF-16.
        // Note the emoji and "𝑖𝜋" consist of UTF-16 surrogate pairs, and we even test
        // chopping at character offsets in the middle of a surrogate pair.
        var stringBuilder = new StringBuilder();
        stringBuilder.Append("🕐;こんにちは! 😀😁😂😃😄😅😆😇");
        int n1 = stringBuilder.Length;
        for (int i = 0; i < 25; ++i)
        {
            stringBuilder.Append(" exp(𝑖𝜋) = -1 ▶▶▶ ");
            stringBuilder.Append("En mathématiques, l'identité d'Euler est une relation " +
                                 "entre plusieurs constantes fondamentales et utilisant " +
                                 "les trois opérations arithmétiques d'addition, " +
                                 "multiplication et exponentiation ☮");
        }
        int n2 = stringBuilder.Length;
        stringBuilder.Append(" 💣 💣 💣");
        int n3 = stringBuilder.Length;

        var testString = stringBuilder.ToString();
        ps.BindParameter(1, testString);

        (int Offset, int Length)[] samples =
        [
            (0, 0), (0, 4), (0, 1), (1, 1), (1, 2), (1, 3), (3, 8),
            (9, 3), (12, 3), (16, 4),
            (n2 / 500, 256), (n2 / 500 + 500, 257), (n2 / 500 + 300, 500),
            (500, 500), (1100, 200), (1600, 400), (2020, 2),
            (n3 - 10, 4), (n3 - 4, 1), (n3 - 4, 2), (n3 - 4, 3), (n3 - 3, 1), (n3 - 3, 2), 
            (n3 - 2, 1), (n3 - 2, 2), (n3 - 1, 1), (n3 - 1, 0), (n3, 0),
            (n3 - 10, 20)
        ];

        using var adoReader = ps.ExecuteReader();
        adoReader.Read();

        void Check((int Offset, int Length)[] samples)
        {
            var buffer = new char[samples.Select(s => s.Length).Max()];
            foreach (var (offset, length) in samples)
            {
                var charsWritten = (int)adoReader.GetChars(0, offset, buffer, 0, length);
                var actualLength = Math.Min(length, testString.Length - offset);
                Assert.Equal(actualLength, charsWritten);
                Assert.Equal(testString.AsSpan().Slice(offset, actualLength),
                             buffer.AsSpan()[..actualLength]);
            }
        }

        Check(samples);

        // Shuffle elements to screw up the internal UTF-8/UTF-16 position cache
        new Random(Seed: 37).Shuffle(samples);
        Check(samples);
    }

}
