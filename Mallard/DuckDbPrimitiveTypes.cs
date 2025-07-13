using System;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// DuckDB's representation of a date.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct DuckDbDate(int days)
{
    /// <summary>
    /// Number of days since 1970-01-01 (Unix epoch).
    /// </summary>
    public int Days = days;
}

/// <summary>
/// DuckDB's representation of a timestamp.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="DuckDbTimestamp" /> class with the specified timestamp value in
/// microseconds.
/// </remarks>
/// <param name="microseconds">The timestamp value represented as the number of microseconds since the Unix epoch (January 1, 1970, 00:00:00
/// UTC).</param>
[StructLayout(LayoutKind.Sequential)]
public struct DuckDbTimestamp(long microseconds)
{
    /// <summary>
    /// Number of microseconds elapsed since midnight 1970-01-01 (Unix epoch).
    /// </summary>
    public long Microseconds = microseconds;
}
