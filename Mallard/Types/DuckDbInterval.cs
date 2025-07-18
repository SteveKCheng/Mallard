using System;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// Time interval in DuckDB.
/// </summary>
/// <remarks>
/// Note that the DuckDB's time intervals use 3 basis units: months, days, microseconds,
/// unlike .NET <see cref="TimeSpan" /> type.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public struct DuckDbInterval
{
    /// <summary>
    /// The number of months to offset by in time calculations.
    /// </summary>
    public int Months;

    /// <summary>
    /// The number of days to offset by in time calculations.
    /// </summary>
    public int Days;

    /// <summary>
    /// The number of microseconds to offset by in time calculations.
    /// </summary>
    public int Microseconds;

    /// <summary>
    /// Convert to a .NET time span if possible.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">
    /// This instance contains a month component (<see cref="Months" /> is non-zero),
    /// which cannot be represented in a <see cref="TimeSpan" />
    /// (since the number of days in a month differs between months).
    /// </exception>
    public readonly TimeSpan ToTimeSpan()
    {
        if (Months != 0)
            throw new InvalidOperationException("Cannot convert a DuckDbInterval with non-zero months into a .NET TimeSpan. ");

        return new TimeSpan(days: Days, 
                            hours: 0, 
                            minutes: 0, 
                            seconds: 0, 
                            milliseconds: 0, 
                            microseconds: Microseconds);
    }


    /// <summary>
    /// Convert from a .NET time span.
    /// </summary>
    /// <param name="timeSpan">
    /// The desired time span to convert to the DuckDB representation.
    /// </param>
    /// <param name="exact">
    /// If true, fail if <paramref name="timeSpan" /> cannot be represented exactly in DuckDB.
    /// If false, the timestamp will be silently rounded to the nearest time interval in microseconds 
    /// for storage into DuckDB.  
    /// </param>
    /// <returns>
    /// The DuckDB interval.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// There are fractional microseconds and <paramref name="exact" /> is true.
    /// </exception>
    public static DuckDbInterval FromTimeSpan(TimeSpan timeSpan, bool exact = true)
    {
        // N.B. days and microseconds have the same sign owing to the rounding behavior
        // of integer division
        var days = (int)Math.DivRem(timeSpan.Ticks, TimeSpan.TicksPerDay, out var intradayTicks);
        var microseconds = DuckDbTimestamp.ConvertTicksToMicroseconds(intradayTicks, exact);

        return new DuckDbInterval
        {
            Days = days,
            Microseconds = (int)microseconds
        };
    }
}
