using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard.Types;

/// <summary>
/// DuckDB's representation of a floating-point decimal number.
/// </summary>
/// <remarks>
/// <para>
/// This structure does not represent the form of decimal numbers that DuckDB encodes in its vectors
/// (which are instead <see cref="Int16"/>, <see cref="Int32" />, <see cref="Int64" />, <see cref="Int128" />
/// depending on the width of the DECIMAL type set on the column).  Rather it is the most general form
/// of decimal number that may be read or written to DuckDB.  
/// </para>
/// <para>
/// In this library, this type may be used
/// as an alternative to the standard <see cref="Decimal" /> type.
/// Some decimal numbers that are very large in magnitude (taking more than 96 bits) are
/// not representable in <see cref="Decimal" />.  Use this type if you need to read or write
/// such decimal numbers.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDbDecimal
{
    // CONSIDER: implement generic math & other interfaces that Decimal implements

    private readonly byte _width;
    private readonly byte _scale;
    private readonly DuckDbUInt128 _value;
        
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
    /// Instantiate <see cref="Decimal" /> with a 16-bit significand
    /// (e.g. as obtained from a DuckDB vector).
    /// </summary>
    /// <param name="value">Signed value of the significand. </param>
    /// <param name="scale">Number of digits after the decimal point.
    /// </param>
    public static Decimal ConvertToDecimal(short value, byte scale)
    {
        var valueInt = (int)value;
        var isNegative = (value < 0);
        var magnitude = isNegative ? -valueInt : valueInt;
        return new Decimal(magnitude, 0, 0, isNegative, scale);
    }

    /// <summary>
    /// Instantiate <see cref="Decimal" /> with a 32-bit significand
    /// (e.g. as obtained from a DuckDB vector).
    /// </summary>
    /// <param name="value">Signed value of the significand. </param>
    /// <param name="scale">Number of digits after the decimal point.
    /// </param>
    public static Decimal ConvertToDecimal(int value, byte scale)
    {
        unchecked // overflow below has expected behavior
        {
            var isNegative = (value < 0);
            var magnitude = isNegative ? (uint)-value : (uint)value;
            return new Decimal((int)magnitude, 0, 0, isNegative, scale);
        }
    }

    /// <summary>
    /// Instantiate <see cref="Decimal" /> with a 64-bit significand
    /// (e.g. as obtained from a DuckDB vector).
    /// </summary>
    /// <param name="value">Signed value of the significand. </param>
    /// <param name="scale">Number of digits after the decimal point.
    /// </param>
    public static Decimal ConvertToDecimal(long value, byte scale)
    {
        unchecked // overflow below has expected behavior
        {
            var isNegative = (value < 0);
            var magnitude = isNegative ? (ulong)-value : (ulong)value;
            var magnitudeLow = (uint)magnitude;
            var magnitudeMid = (uint)(magnitude >> 32);
            return new Decimal((int)magnitudeLow, (int)magnitudeMid, 0, isNegative, scale);
        }
    }

    /// <summary>
    /// Instantiate <see cref="Decimal" /> with a full 128-bit significand.
    /// </summary>
    /// <param name="value">Signed value of the significand. </param>
    /// <param name="scale">Number of digits after the decimal point.
    /// </param>
    public static Decimal ConvertToDecimal(Int128 value, byte scale)
        => new DuckDbDecimal(value, scale, 38 /* ignored */).ToDecimal();

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
    /// when specifying actual <em>values</em> to its API.  If the smallest possible width
    /// is required, that can obviously be calculated from the significand.  
    /// </para>
    /// <para>
    /// Gleaning from the source code of DuckDB, the width may be used to determine the smallest
    /// integer type that can hold the significand, so that it can be processed and stored
    /// efficiently.  There are only 4 integer types used, according to DuckDB's documentation:
    /// INT16, INT32, INT64, and INT128.  Therefore we only bucket the significand into one
    /// of those 4 classes, and not try to find the exact number of decimal digits in the 
    /// significand.
    /// </para>
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

    #region Type conversions for vector reader

    /// <summary>
    /// Read a decimal value encoded inside a DuckDB vector and convert to a .NET value.
    /// </summary>
    /// <remarks>
    /// This generic function is to be instantiated for a finite set of cases (all known at compile-time).
    /// The genericity is only to reduce code duplication.
    /// </remarks>
    /// <typeparam name="TStorage">
    /// The storage type of the decimal values inside the DuckDB vector.
    /// One of: <see cref="Int16" />, <see cref="Int32" />, <see cref="Int64" />, <see cref="Int128" />.
    /// </typeparam>
    /// <typeparam name="TResult">
    /// The .NET result type of the conversion.   
    /// Must be the same as <typeparamref name="TUnderlyingResult" />,
    /// or the nullable version of that, or <see cref="System.Object" />.
    /// </typeparam>
    /// <typeparam name="TUnderlyingResult">
    /// Either <see cref="DuckDbDecimal" /> or <see cref="Decimal" />.
    /// </typeparam>
    private static TResult ConvertFromVector<TStorage, TResult, TUnderlyingResult>(object? state, in DuckDbVectorInfo vector, int index)
        where TStorage: unmanaged, IBinaryInteger<TStorage>
        where TUnderlyingResult: struct
    {
        var decimalScale = vector.ColumnInfo.DecimalScale;

        if (typeof(TUnderlyingResult) == typeof(DuckDbDecimal))
        {
            var decimalWidth = (byte)vector.ColumnInfo.ElementSize;
            var value = new DuckDbDecimal(Int128.CreateTruncating(vector.UnsafeRead<TStorage>(index)),
                                          decimalScale, decimalWidth);
            if (typeof(TResult) == typeof(DuckDbDecimal) || typeof(TResult) == typeof(object))
                return (TResult)(object)value;
            if (typeof(TResult) == typeof(DuckDbDecimal?))
                return (TResult)(object)new Nullable<DuckDbDecimal>(value);
        }

        if (typeof(TUnderlyingResult) == typeof(Decimal))
        {
            // Converting to Decimal directly without going through DuckDbDecimal is faster.
            // e.g. Most applications do not use 128-bit storage which eliminates all
            //      exception-throwing code paths.

            Decimal value;
            if (typeof(TStorage) == typeof(Int16))
                value = ConvertToDecimal(vector.UnsafeRead<Int16>(index), decimalScale);
            else if (typeof(TStorage) == typeof(Int32))
                value = ConvertToDecimal(vector.UnsafeRead<Int32>(index), decimalScale);
            else if (typeof(TStorage) == typeof(Int64))
                value = ConvertToDecimal(vector.UnsafeRead<Int64>(index), decimalScale);
            else if (typeof(TStorage) == typeof(Int128))
                value = ConvertToDecimal(vector.UnsafeRead<Int128>(index), decimalScale);
            else
                throw new UnreachableException();

            if (typeof(TResult) == typeof(Decimal) || typeof(TResult) == typeof(object))
                return (TResult)(object)value;
            if (typeof(TResult) == typeof(Decimal?))
                return (TResult)(object)new Nullable<Decimal>(value);
        }

        throw new UnreachableException();
    }

    private unsafe static VectorElementConverter GetConverterGeneric<TResult, TUnderlyingResult>(in DuckDbColumnInfo column)
        where TUnderlyingResult : struct
        => column.StorageKind switch
        {
            DuckDbValueKind.SmallInt => VectorElementConverter.Create(&ConvertFromVector<Int16, TResult, TUnderlyingResult>),
            DuckDbValueKind.Integer => VectorElementConverter.Create(&ConvertFromVector<Int32, TResult, TUnderlyingResult>),
            DuckDbValueKind.BigInt => VectorElementConverter.Create(&ConvertFromVector<Int64, TResult, TUnderlyingResult>),
            DuckDbValueKind.HugeInt => VectorElementConverter.Create(&ConvertFromVector<Int128, TResult, TUnderlyingResult>),
            _ => throw new InvalidOperationException("Cannot decode Decimal from a DuckDB vector with the given storage type. ")
        };

    internal static VectorElementConverter GetConverterForDuckDbDecimal(in DuckDbColumnInfo column)
        => GetConverterGeneric<DuckDbDecimal, DuckDbDecimal>(column);
    internal static VectorElementConverter GetConverterForNullableDuckDbDecimal(in DuckDbColumnInfo column)
        => GetConverterGeneric<DuckDbDecimal?, DuckDbDecimal>(column);
    internal static VectorElementConverter GetConverterForBoxedDuckDbDecimal(in DuckDbColumnInfo column)
        => GetConverterGeneric<object, DuckDbDecimal>(column);

    internal static VectorElementConverter GetConverterForDecimal(in DuckDbColumnInfo column)
        => GetConverterGeneric<Decimal, Decimal>(column);
    internal static VectorElementConverter GetConverterForNullableDecimal(in DuckDbColumnInfo column)
        => GetConverterGeneric<Decimal?, Decimal>(column);
    internal static VectorElementConverter GetConverterForBoxedDecimal(in DuckDbColumnInfo column)
        => GetConverterGeneric<object, Decimal>(column);

    #endregion
}
