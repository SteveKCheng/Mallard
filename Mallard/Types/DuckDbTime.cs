using System;

namespace Mallard;

/// <summary>
/// DuckDB's representation of a time of day.
/// </summary>
/// <param name="microseconds">The time of day represented as the number of microseconds since midnight.
/// </param>
/// <remarks>
/// This type exists mainly to enable time-of-day values reported by DuckDB to be read
/// directly from the memory of a DuckDB vector.  The fields are not wrapped in properties
/// to allow vectorized processing (i.e. SIMD), and DuckDB already essentially guarantees a stable
/// layout of this structure.
/// </remarks>
public readonly struct DuckDbTime(long microseconds) : IStatelesslyConvertible<DuckDbTime, TimeOnly>
{
    /// <summary>
    /// Number of microseconds elapsed since midnight.
    /// </summary>
    public readonly long Microseconds = microseconds;
    
    /// <summary>
    /// Convert to a <see cref="TimeOnly" /> instance.
    /// </summary>
    public TimeOnly ToTimeOnly() => new TimeOnly(ticks: Microseconds * TimeSpan.TicksPerMicrosecond);
    
    static TimeOnly IStatelesslyConvertible<DuckDbTime, TimeOnly>.Convert(ref readonly DuckDbTime item)
        => item.ToTimeOnly(); 
}
