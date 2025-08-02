using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.Tests;

public class TestRowOriented(DatabaseFixture fixture) : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _fixture = fixture;
    private DuckDbConnection DbConnection => _fixture.DbConnection;

    [Fact]
    public void DataReader()
    {
        const int limitRows = 200;
        using var ps = DbConnection.CreatePreparedStatement($"SELECT * FROM orders LIMIT {limitRows}");


        var adoReader = ps.ExecuteReader();

        var columnNames = new string[]
        {
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment"
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
            // Columns:
            //   o_orderkey BIGINT
            //   o_custkey BIGINT
            //   o_orderstatus VARCHAR
            //   o_totalprice DECIMAL(15,2)
            //   o_orderdate DATE
            //   o_orderpriority VARCHAR
            //   o_clerk VARCHAR
            //   o_shippriority INTEGER
            //   o_comment VARCHAR
            var o_orderkey = chunkReader.GetColumn<long>(0);
            var o_custkey = chunkReader.GetColumn<long>(1);
            var o_orderstatus = chunkReader.GetColumn<string>(2);
            var o_totalprice = chunkReader.GetColumn<decimal>(3);
            var o_orderdate = chunkReader.GetColumn<DateOnly>(4);
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
                Assert.Equal(o_orderdate.GetItem(i), adoReader.GetFieldValue<DateOnly>(4));
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

}
