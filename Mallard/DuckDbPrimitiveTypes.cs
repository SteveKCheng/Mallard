using System;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// DuckDB's representation of a date.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct DuckDbDate
{
    /// <summary>
    /// Number of days since 1970-01-01 (Unix epoch).
    /// </summary>
    public int Days;
}

/// <summary>
/// DuckDB's representation of a timestamp.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct DuckDbTimestamp
{
    /// <summary>
    /// Number of microseconds elapsed since midnight 1970-01-01 (Unix epoch).
    /// </summary>
    public long Microseconds;

    /// <summary>
    /// Initializes a new instance of the <see cref="DuckDbTimestamp" /> class with the specified timestamp value in
    /// microseconds.
    /// </summary>
    /// <param name="microseconds">The timestamp value represented as the number of microseconds since the Unix epoch (January 1, 1970, 00:00:00
    /// UTC).</param>
    public DuckDbTimestamp(long microseconds)
    {
        Microseconds = microseconds;
    }
}
