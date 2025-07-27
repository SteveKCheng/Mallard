using Mallard.DataFrames;
using Microsoft.Data.Analysis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.Tests;

public class TestDataFrames
{
    [Fact]
    public void PopulateDataFrame()
    {
        using var dbConn = new DuckDbConnection("");

        var testFilePath = Path.Join(Program.TestDataDirectory, "QQQ.parquet");
        dbConn.ExecuteNonQuery($"CREATE TEMP TABLE PriceQQQ AS SELECT * FROM read_parquet('{testFilePath}')");

        using var dbResult = dbConn.Execute(@"
            SELECT 
                Date, 
                CAST(Open AS DOUBLE) AS Open,
                CAST(Close AS DOUBLE) AS Close,
                CAST(Low AS DOUBLE) As Low,
                CAST(High AS DOUBLE) As High,
                Volume
            FROM PriceQQQ
                ORDER BY Date ASC");

        var dataFrame = DataFrameExtensions.Create(dbResult);

        Assert.Equal(6, dataFrame.Columns.Count);

        Assert.IsAssignableFrom<PrimitiveDataFrameColumn<DateOnly>>(dataFrame.Columns[0]);
        for (int i = 1; i <= 4; ++i)
            Assert.IsAssignableFrom<PrimitiveDataFrameColumn<double>>(dataFrame.Columns[i]);

        var closes = Assert.IsAssignableFrom<PrimitiveDataFrameColumn<double>>(dataFrame["Close"]);
        
        var peakCloses = closes.CumulativeMax();
        for (long i = 0; i < peakCloses.Length; ++i)
            Assert.True((double)peakCloses[i] > 0);
    }
}
