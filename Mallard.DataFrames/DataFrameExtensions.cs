using Microsoft.Data.Analysis;
using System;
using System.Linq;

namespace Mallard.DataFrames;
using Mallard.Types;
using DataFrameColumnAndReader = (DataFrameColumn Output, Action<DataFrameColumn, DuckDbChunkReader, int> ReadAction);
   
/// <summary>
/// Extension methods for working with data frames together with DuckDB. 
/// </summary>
public static class DataFrameExtensions
{
    /// <summary>
    /// Copy the results from a DuckDB query into a new data frame. 
    /// </summary>
    /// <param name="queryResult">
    /// The results of a DuckDB query.  As advancing through the results modifies the object,
    /// be sure that no other thread is using the same object while this method executes.
    /// </param>
    /// <returns>
    /// A newly-instantiated data frame holding a copy of the DuckDB results.
    /// </returns>
    /// <exception cref="NotSupportedException">
    /// The results contain data types that cannot be put into a data frame.
    /// </exception>
    public static DataFrame Create(DuckDbResult queryResult)
    {
        var columns = new DataFrameColumnAndReader[queryResult.ColumnCount];

        for (int columnIndex = 0; columnIndex < columns.Length; ++columnIndex)
        {
            var columnInfo = queryResult.GetColumnInfo(columnIndex);
            var columnName = queryResult.GetColumnName(columnIndex);
            columns[columnIndex] = columnInfo.ValueKind switch
            {
                DuckDbValueKind.Boolean => GetColumnAndReaderRaw<bool>(columnName),
                DuckDbValueKind.TinyInt => GetColumnAndReaderRaw<sbyte>(columnName),
                DuckDbValueKind.SmallInt => GetColumnAndReaderRaw<short>(columnName),
                DuckDbValueKind.Integer => GetColumnAndReaderRaw<int>(columnName),
                DuckDbValueKind.BigInt => GetColumnAndReaderRaw<long>(columnName),

                DuckDbValueKind.UTinyInt => GetColumnAndReaderRaw<byte>(columnName),
                DuckDbValueKind.USmallInt => GetColumnAndReaderRaw<ushort>(columnName),
                DuckDbValueKind.UInteger => GetColumnAndReaderRaw<uint>(columnName),
                DuckDbValueKind.UBigInt => GetColumnAndReaderRaw<ulong>(columnName),

                DuckDbValueKind.Float => GetColumnAndReaderRaw<float>(columnName),
                DuckDbValueKind.Double => GetColumnAndReaderRaw<double>(columnName),
                
                DuckDbValueKind.Date => GetColumnAndReader<DateOnly>(columnName),
                DuckDbValueKind.Timestamp => GetColumnAndReader<DateTime>(columnName),

                DuckDbValueKind.VarChar => GetStringColumnAndReader(columnName),

                DuckDbValueKind.Uuid => GetColumnAndReader<Guid>(columnName),
                
                DuckDbValueKind.Decimal => GetColumnAndReader<decimal>(columnName),

                _ => throw new NotSupportedException(
                    $"The DuckDB query result contains a column of type {columnInfo.ValueKind} " +
                    "which is not supported in creating a DataFrame. ")
            };
        }

        queryResult.ProcessAllChunks(columns, static (in DuckDbChunkReader reader, DataFrameColumnAndReader[] columns) =>
        {
            for (int columnIndex = 0; columnIndex < columns.Length; ++columnIndex)
            {
                var (output, readAction) = columns[columnIndex];
                readAction(output, reader, columnIndex);
            }
            return false;   // unused
        });

        return new DataFrame(columns.Select(c => c.Output));
    }

    private static DataFrameColumnAndReader GetColumnAndReaderRaw<T>(string name) where T : unmanaged
    {
        static void Read(DataFrameColumn target, DuckDbChunkReader reader, int columnIndex)
                            => ((PrimitiveDataFrameColumn<T>)target).AppendFrom(reader.GetColumnRaw<T>(columnIndex));
        return (new PrimitiveDataFrameColumn<T>(name), Read);
    }

    private static DataFrameColumnAndReader GetStringColumnAndReader(string name)
    {
        static void Read(DataFrameColumn target, DuckDbChunkReader reader, int columnIndex)
                            => ((StringDataFrameColumn)target).AppendFrom(reader.GetColumnRaw<DuckDbString>(columnIndex));
        return (new StringDataFrameColumn(name), Read);
    }

    private static DataFrameColumnAndReader GetColumnAndReader<T>(string name) where T : unmanaged
    {
        static void Read(DataFrameColumn target, DuckDbChunkReader reader, int columnIndex)
                            => ((PrimitiveDataFrameColumn<T>)target).AppendFrom(reader.GetColumn<T>(columnIndex));
        return (new PrimitiveDataFrameColumn<T>(name), Read);
    }
}
