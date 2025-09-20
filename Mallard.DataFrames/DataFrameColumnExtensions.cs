using Microsoft.Data.Analysis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.DataFrames;
using Mallard.Types;

/// <summary>
/// Extension methods for working with data frame columns together with DuckDB. 
/// </summary>
public static class DataFrameColumnExtensions
{
    /// <summary>
    /// Append items from a DuckDB vector into a data frame's column.
    /// </summary>
    /// <param name="column">The column to append into. </param>
    /// <param name="reader">The DuckDB vector to read items from. </param>
    /// <typeparam name="T">The element type of the items. </typeparam>
    public static void AppendFrom<T>(this PrimitiveDataFrameColumn<T> column, in DuckDbVectorRawReader<T> reader) 
        where T : unmanaged
    {
        for (int i = 0; i < reader.Length; ++i)
        {
            bool valid = reader.TryGetItem(i, out var value);
            column.Append(valid ? value : null);
        }
    }

    /// <summary>
    /// Append items from a DuckDB vector into a data frame's column.
    /// </summary>
    /// <param name="column">The column to append into. </param>
    /// <param name="reader">The DuckDB vector to read items from. </param>
    /// <typeparam name="T">The element type of the items, which must have the same representation
    /// as what DuckDB uses natively in its vectors.
    /// </typeparam>
    public static void AppendFrom<T>(this PrimitiveDataFrameColumn<T> column, in DuckDbVectorReader<T> reader) 
        where T : unmanaged
    {
        for (int i = 0; i < reader.Length; ++i)
        {
            bool valid = reader.TryGetItem(i, out var value);
            column.Append(valid ? value : null);
        }
    }

    /// <summary>
    /// Append items from a string-valued DuckDB vector into a data frame column for strings.
    /// </summary>
    /// <param name="column">The column to append into. </param>
    /// <param name="reader">The DuckDB vector to read items from. </param>
    public static void AppendFrom(this StringDataFrameColumn column, in DuckDbVectorRawReader<DuckDbString> reader)
    {
        for (int i = 0; i < reader.Length; ++i)
        {
            bool valid = reader.TryGetItem(i, out var value);
            column.Append(valid ? value.ToString() : null);
        }
    }
}
