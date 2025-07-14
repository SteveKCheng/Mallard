using Mallard.C_API;
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
/// <param name="microseconds">The timestamp value represented as the number of microseconds since the Unix epoch (January 1, 1970, 00:00:00
/// UTC).</param>
/// <remarks>
/// This type exists mainly to enable date/time values reported by DuckDB to be read
/// directly from the memory of a DuckDB vector. 
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public struct DuckDbTimestamp(long microseconds)
{
    /// <summary>
    /// Number of microseconds elapsed since midnight 1970-01-01 (Unix epoch).
    /// </summary>
    public long Microseconds = microseconds;

    /// <summary>
    /// Convert from a standard <see cref="DateTime" />.
    /// </summary>
    /// <param name="dateTime">Desired date/time to represent in DuckDB. </param>
    /// <param name="exact">
    /// If true, fail if <paramref name="dateTime" /> cannot be represented exactly in DuckDB.
    /// If false, the timestamp will be silently rounded to the nearest timepoint in milliseconds 
    /// (since the epoch) for storage into DuckDB.  (The earliest and latest times allowed in 
    /// <see cref="DateTime" /> are both representable in DuckDB, so underflow or overflow cannot
    /// occur when converting to <see cref="DuckDbTimestamp" />, only rounding.)
    /// </param>
    /// <returns>The DuckDB representation of the date/time. </returns>
    public static DuckDbTimestamp FromDateTime(DateTime dateTime, bool exact = true)
    {
        // Under the current internal representation of DataTime, the following
        // calculation cannot underflow: dateTime.Ticks is always non-negative.
        // Note that the TimeSpan calculation ignores DateTimeKind.
        var dt = (dateTime - DateTime.UnixEpoch).Ticks;

        var a = Math.DivRem(dt, TimeSpan.TicksPerMicrosecond, out var r);
        if (r != 0)
        {
            if (exact)
                throw new ArgumentException("The given DateTime instance is not exactly representable in DuckDB as a timestamp. ");

            const long h = TimeSpan.TicksPerMicrosecond / 2;

            // Adjust so that the division is round-to-even (statistical/banker's rounding).
            // Note that Math.DivRem rounds the quotient towards zero.
            if (a > 0) // r > 0
            {
                var s = ((r > h) ? 1 : 0) - ((r < h) ? 1 : 0);  // sign of r-h
                a = (s == 0) ? ((a + 1) & ~1L) : a + s; 
            }
            else // a < 0, r < 0
            {
                var s = ((0 > h+r) ? 1 : 0) - ((0 < h+r) ? 1 : 0); // sign of |r|-h
                a = (s == 0) ? (a & ~1L) : a - s;
            }
        }

        return new DuckDbTimestamp(a);
    }

    /// <summary>
    /// Convert to a standard <see cref="DateTime" />.
    /// </summary>
    /// <returns>The DateTime instance that represents the same point in time as this DuckDB timestamp. 
    /// Its <see cref="DateTimeKind" /> will be set to <see cref="DateTimeKind.Unspecified" />.
    /// </returns>
    /// <exception cref="OverflowException">
    /// When this DuckDB timestamp cannot be represented in an instance of <see cref="DateTime" />,
    /// because the time point exists earlier than (underflows) <see cref="DateTime.MinValue" /> 
    /// or later than (overflows) <see cref="DateTime.MaxValue" />.  (<see cref="DateTime" /> has finer
    /// resolution than microseconds, so the conversion is always exact if there is no does not overflow
    /// or underflow.)
    /// </exception>
    public readonly DateTime ToDateTime()
    {
        // Shift the time origin in units of microseconds first, not ticks,
        // to avoid one source of overflow/underflow.  The divisions by
        // TimeSpan.TicksPerMicrosecond here and below should be readily optimized
        // by the compiler since the dividends are constant.
        var epochMicroseconds = (DateTime.UnixEpoch.Ticks / TimeSpan.TicksPerMicrosecond);

        long t = 0;
        bool overflow = false;
        try
        {
            t = checked(epochMicroseconds + Microseconds);
        }
        catch (OverflowException)
        {
            overflow = true;
        }

        if (overflow ||
            t > DateTime.MaxValue.Ticks / TimeSpan.TicksPerMicrosecond ||
            t < DateTime.MinValue.Ticks / TimeSpan.TicksPerMicrosecond)
        {
            throw new OverflowException("The given DuckDB timestamp exceeds the range allowed by DateTime. ");
        }

        return new DateTime(ticks: t * 10, kind: DateTimeKind.Unspecified);
    }
}
