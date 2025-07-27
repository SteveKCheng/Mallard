using Microsoft.Data.Analysis;
using System;
using System.Linq;

namespace Mallard.DataFrames;
using DataFrameColumnAndReader = (DataFrameColumn Output, Action<DataFrameColumn, DuckDbChunkReader, int> ReadAction);
   
public static class DataFrameExtensions
{
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

                DuckDbValueKind.VarChar => GetStringColumnAndReader(columnName),

                DuckDbValueKind.Decimal => GetColumnAndReader<decimal>(columnName),

                _ => throw new NotSupportedException()
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

    public static DataFrameColumnAndReader GetStringColumnAndReader(string name)
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
