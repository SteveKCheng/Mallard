using System;
using System.Runtime.InteropServices;

namespace Mallard.Types;

/// <summary>
/// DuckDB's representation of a date.
/// </summary>
/// <param name="days">
/// The date represented as a number of days since the Unix epoch (January 1, 1970).
/// </param>
/// <remarks>
/// <para>
/// The fields are not wrapped in properties
/// to allow vectorized processing (i.e. SIMD), and DuckDB already essentially guarantees a stable
/// layout of this structure.
/// </para>
/// <para>
/// .NET's <see cref="DateOnly" /> allows dates from 0001-01-01 to 9999-12-31.
/// DuckDB seems to allow dates on or beyond year 10000.  Thus, to be safe, there is no
/// implicit conversion from <see cref="DuckDbDate" /> to <see cref="DateOnly" /> since
/// it could conceivably throw an exception at run-time.  But there is an implicit conversion
/// from <see cref="DateOnly" /> to <see cref="DuckDbDate" />.
/// </para>
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

    /// <summary>
    /// Implicit conversion from <see cref="DateOnly" /> to <see cref="DuckDbDate" />.
    /// </summary>
    /// <param name="date">The desired date. 
    /// </param>
    /// <returns>
    /// The DuckDB representation of the date.  The conversion always succeeds.
    /// </returns>
    public static implicit operator DuckDbDate(DateOnly date) => FromDateOnly(date); 
    
    #region Type conversions for vector reader

    static DateOnly IStatelesslyConvertible<DuckDbDate, DateOnly>.Convert(ref readonly DuckDbDate item) 
        => item.ToDateOnly();

    static DateTime IStatelesslyConvertible<DuckDbDate, DateTime>.Convert(ref readonly DuckDbDate item)
        => item.ToDateOnly().ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified);

    #endregion
}
