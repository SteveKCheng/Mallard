using System;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// DuckDB's representation of a date.
/// </summary>
/// <param name="days">
/// The date represented as a number of days since the Unix epoch (January 1, 1970).
/// </param>
/// <remarks>
/// The fields are not wrapped in properties
/// to allow vectorized processing (i.e. SIMD), and DuckDB already essentially guarantees a stable
/// layout of this structure. 
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public struct DuckDbDate(int days) 
    : IStatelesslyConvertible<DuckDbDate, DateOnly>
    , IStatelesslyConvertible<DuckDbDate, DateTime>
{
    /// <summary>
    /// Number of days since 1970-01-01 (Unix epoch).
    /// </summary>
    public int Days = days;

    /// <summary>
    /// Convert from a standard <see cref="DateOnly" />.
    /// </summary>
    /// <param name="date">
    /// Desired date to represent in DuckDB.
    /// </param>
    /// <returns>
    /// The DuckDB representation of the date.  
    /// </returns>
    public static DuckDbDate FromDateOnly(DateOnly date)
    {
        return new DuckDbDate(date.DayNumber - new DateOnly(1970, 1, 1).DayNumber);
    }

    /// <summary>
    /// Convert this instance to a standard <see cref="DateOnly" />.
    /// </summary>
    public readonly DateOnly ToDateOnly()
    {
        return DateOnly.FromDayNumber(Days + new DateOnly(1970, 1, 1).DayNumber);
    }

    #region Type conversions for vector reader

    static DateOnly IStatelesslyConvertible<DuckDbDate, DateOnly>.Convert(ref readonly DuckDbDate item) 
        => item.ToDateOnly();

    static DateTime IStatelesslyConvertible<DuckDbDate, DateTime>.Convert(ref readonly DuckDbDate item)
        => item.ToDateOnly().ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified);

    #endregion
}
