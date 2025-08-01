﻿using System;

namespace Mallard;

/// <summary>
/// Implementation interface for a chunk to retrieve metadata and other state
/// associated to columns in a DuckDB query result, common to all chunks. 
/// </summary>
/// <remarks>
/// <para>
/// We may need a degree of control over resource management and multi-thread
/// access, so to that end this interface is not exposed to users.
/// </para>
/// </remarks>
internal interface IResultColumns
{
    /// <summary>
    /// Get information about a column in the results.
    /// </summary>
    /// <param name="columnIndex">
    /// The index of the column, between 0 (inclusive) to <see cref="ColumnCount" /> (exclusive).
    /// </param>
    DuckDbColumnInfo GetColumnInfo(int columnIndex);

    /// <summary>
    /// The number of top-level columns present in the results.
    /// </summary>
    int ColumnCount { get; }

    /// <summary>
    /// Get the converter to convert items on some vector for the given column.
    /// </summary>
    /// <param name="columnIndex">The target column. </param>
    /// <param name="targetType">The .NET type to convert items of the vector to. 
    /// If null, the type is implied in the same way as <see cref="VectorElementConverter.CreateForType" />.
    /// </param>
    /// <remarks>
    /// This method exists here so that (parts of) the
    /// converter state may be cached, and re-used across all chunks.
    /// </remarks>
    /// <returns>
    /// <see cref="VectorElementConverter" /> unbound to any particular vector
    /// (so <see cref="VectorElementConverter.BindToVector" /> must be called before
    /// conversion can be invoked).
    /// </returns>
    internal VectorElementConverter GetColumnConverter(int columnIndex, Type? targetType);
}
