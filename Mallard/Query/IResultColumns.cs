using System;

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
    ref readonly DuckDbColumnInfo GetColumnInfo(int columnIndex);

    /// <summary>
    /// The number of top-level columns present in the results.
    /// </summary>
    int ColumnCount { get; }
}
