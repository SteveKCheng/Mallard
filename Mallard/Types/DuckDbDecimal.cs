using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// DuckDB's encoding of a floating-point decimal number.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDbDecimal
{
    private readonly byte _width;
    private readonly byte _scale;
    private readonly DuckDbHugeUInt _value;
        
    /// <summary>
    /// The maximum significand that can be represented by .NET's <see cref="Decimal" /> type: 2^96 - 1.
    /// </summary>
    private static readonly UInt128 MaxSignificand = new(upper: 0xFFFFFFFFu, lower: 0xFFFFFFFFFFFFFFFFul);

    private DuckDbDecimal(Int128 value, byte scale, byte width)
    {
        _width = width;
        _scale = scale;
        _value = new(value);
    }

    /// <summary>
    /// Convert from DuckDB's representation of a decimal number to a .NET <see cref="Decimal" />.
    /// </summary>
    /// <returns>
    /// The converted value.
    /// </returns>
    /// <exception cref="OverflowException">
    /// The decimal value encoded by this instance cannot be fit into .NET's <see cref="Decimal" /> type.
    /// </exception>
    public Decimal ToDecimal()
    {
        unchecked
        {
            var value = _value.ToInt128();
            var isNegative = (value < 0);
            var magnitude = isNegative ? (UInt128)(-value) : (UInt128)value;

            if (magnitude <= MaxSignificand)
            {
                var magnitudeLow = (uint)magnitude;
                var magnitudeMid = (uint)(magnitude >> 32);
                var magnitudeHigh = (uint)(magnitude >> 64);
                return new Decimal((int)magnitudeLow, (int)magnitudeMid, (int)magnitudeHigh,
                                   isNegative, _scale);
            }
            else
            {
                throw new OverflowException("The original value is too large to fit into an instance of Decimal. ");
            }
        }
    }

    /// <summary>
    /// Find a decimal width that accomodates the given significand to provide to DuckDB.
    /// </summary>
    /// <remarks>
    /// <para>
    /// It is not clear to this author why DuckDB needs the decimal width to be provided 
    /// when specifying actual <i>values</i> to its API.  If the smallest possible width
    /// is required, that can obviously be calculated from the significand.  
    /// </para>
    /// <para>
    /// Gleaning from the source code of DuckDB, the width may be used to determine the smallest
    /// integer type that can hold the significand, so that it can be processed and stored
    /// efficiently.  There are only 4 integer types used, according to DuckDB's documentation:
    /// INT16, INT32, INT64, and INT128.  Therefore we only bucket the significand into one
    /// of those 4 classes, and not try to find the exact number of decimal digits in the 
    /// significand.
    /// </remarks>
    /// <param name="value">Absolute value of the significand. </param>
    /// <returns>
    /// A decimal width that can accomodate the given significand.
    /// </returns>
    private static byte ApproximateWidth(UInt128 value)
    {
        if (value <= 9999u) return 4;                      // assume 16 bits
        if (value <= 999_999_999u) return 9;               // assume 32 bits
        if (value <= 999_999_999_999_999_99ul) return 18;  // assume 64 bits
        return 38;                                         // assume full 128 bits
    }

    /// <summary>
    /// Convert a .NET <see cref="Decimal"/> to a DuckDB decimal.
    /// </summary>
    /// <param name="input">The value to convert. </param>
    /// <returns>DuckDB representation of the value. </returns>
    public static DuckDbDecimal FromDecimal(Decimal input)
    {
        var significand = ExtractSignificand(input);
        return new DuckDbDecimal(value: (input < 0) ? -(Int128)significand : (Int128)significand,
                                 scale: input.Scale,
                                 width: ApproximateWidth(significand));
    }

    /// <summary>
    /// Extract the 128-bit significand from a floating-point-like value.
    /// </summary>
    /// <typeparam name="T">
    /// A numeric type that implements <see cref="IFloatingPoint{T}"/>.
    /// </typeparam>
    /// <param name="input">
    /// The value to examine.
    /// </param>
    /// <returns>The (absolute value of the) significand. </returns>
    /// <exception cref="OverflowException">
    /// The significand is larger than what can be represented
    /// by the return value of this method.
    /// </exception>
    /// <remarks>
    /// This method is used internally to extract the significand
    /// of <see cref="Decimal" />.  There is no other public API to do other than the 
    /// interface methods of the "generic math" feature of .NET.  Assumes a little-endian
    /// architecture, as DuckDB does.
    /// </remarks>
    private unsafe static UInt128 ExtractSignificand<T>(T input) where T : IFloatingPoint<T>
    {
        if (input.GetSignificandByteCount() > sizeof(UInt128))
            throw new OverflowException("The significand of the value is larger than what can be represented by a 128-bit integer. ");

        // N.B. In little-endian representation, UInt128 significand has the correct value
        // even if bytesWritten < sizeof(UInt128), the significand is padded with zeroes at the end.
        UInt128 significand = default;
        input.TryWriteSignificandLittleEndian(
            new Span<byte>(Unsafe.AsPointer(ref significand), sizeof(UInt128)), out _);

        return significand;
    }
}
