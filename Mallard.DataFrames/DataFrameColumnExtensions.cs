using Microsoft.Data.Analysis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.DataFrames;
using Mallard.Types;

public static class DataFrameColumnExtensions
{
    public static void AppendFrom<T>(this PrimitiveDataFrameColumn<T> column, in DuckDbVectorRawReader<T> reader) 
        where T : unmanaged
    {
        for (int i = 0; i < reader.Length; ++i)
        {
            bool valid = reader.TryGetItem(i, out var value);
            column.Append(valid ? value : null);
        }
    }

    public static void AppendFrom<T>(this PrimitiveDataFrameColumn<T> column, in DuckDbVectorReader<T> reader) 
        where T : unmanaged
    {
        for (int i = 0; i < reader.Length; ++i)
        {
            bool valid = reader.TryGetItem(i, out var value);
            column.Append(valid ? value : null);
        }
    }

    public static void AppendFrom(this StringDataFrameColumn column, in DuckDbVectorRawReader<DuckDbString> reader)
    {
        for (int i = 0; i < reader.Length; ++i)
        {
            bool valid = reader.TryGetItem(i, out var value);
            column.Append(valid ? value.ToString() : null);
        }
    }
}
