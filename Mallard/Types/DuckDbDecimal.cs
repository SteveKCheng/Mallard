﻿using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard;

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

    #region Type conversions for vector reader

    /// <summary>
    /// Read a decimal value encoded inside a DuckDB vector and converto to a .NET <see cref="Decimal" />.
    /// </summary>
    /// <typeparam name="TStorage">
    /// The storage type of the decimal values inside the DuckDB vector.
    /// </typeparam>
    private static Decimal ConvertToDecimalFromVector<TStorage>(object? state, in DuckDbVectorInfo vector, int index)
    {
        var decimalScale = vector.ColumnInfo.DecimalScale;
        if (typeof(TStorage) == typeof(Int16))
            return ConvertToDecimal(vector.UnsafeRead<Int16>(index), decimalScale);
        else if (typeof(TStorage) == typeof(Int32))
            return ConvertToDecimal(vector.UnsafeRead<Int32>(index), decimalScale);
        else if (typeof(TStorage) == typeof(Int64))
            return ConvertToDecimal(vector.UnsafeRead<Int64>(index), decimalScale);
        else if (typeof(TStorage) == typeof(Int128))
            return ConvertToDecimal(vector.UnsafeRead<Int128>(index), decimalScale);
        else
            throw new UnreachableException();
    }

    private static object ConvertToBoxedDecimalFromVector<TStorage>(object? state, in DuckDbVectorInfo vector, int index)
        => (object)ConvertToDecimalFromVector<TStorage>(state, vector, index);

    private static Decimal? ConvertToNullableDecimalFromVector<TStorage>(object? state, in DuckDbVectorInfo vector, int index)
        => new Nullable<Decimal>(ConvertToDecimalFromVector<TStorage>(state, vector, index));

    internal unsafe static VectorElementConverter GetVectorElementConverter(in DuckDbColumnInfo column)
        => column.StorageKind switch
        {
            DuckDbValueKind.SmallInt => VectorElementConverter.Create(&ConvertToDecimalFromVector<Int16>),
            DuckDbValueKind.Integer => VectorElementConverter.Create(&ConvertToDecimalFromVector<Int32>),
            DuckDbValueKind.BigInt => VectorElementConverter.Create(&ConvertToDecimalFromVector<Int64>),
            DuckDbValueKind.HugeInt => VectorElementConverter.Create(&ConvertToDecimalFromVector<Int128>),
            _ => throw new InvalidOperationException("Cannot decode Decimal from a DuckDB vector with the given storage type. ")
        };

    internal unsafe static VectorElementConverter GetBoxedVectorElementConverter(in DuckDbColumnInfo column)
        => column.StorageKind switch
        {
            DuckDbValueKind.SmallInt => VectorElementConverter.Create(&ConvertToBoxedDecimalFromVector<Int16>),
            DuckDbValueKind.Integer => VectorElementConverter.Create(&ConvertToBoxedDecimalFromVector<Int32>),
            DuckDbValueKind.BigInt => VectorElementConverter.Create(&ConvertToBoxedDecimalFromVector<Int64>),
            DuckDbValueKind.HugeInt => VectorElementConverter.Create(&ConvertToBoxedDecimalFromVector<Int128>),
            _ => throw new InvalidOperationException("Cannot decode Decimal from a DuckDB vector with the given storage type. ")
        };

    internal unsafe static VectorElementConverter GetNullableVectorElementConverter(in DuckDbColumnInfo column)
        => column.StorageKind switch
        {
            DuckDbValueKind.SmallInt => VectorElementConverter.Create(&ConvertToNullableDecimalFromVector<Int16>),
            DuckDbValueKind.Integer => VectorElementConverter.Create(&ConvertToNullableDecimalFromVector<Int32>),
            DuckDbValueKind.BigInt => VectorElementConverter.Create(&ConvertToNullableDecimalFromVector<Int64>),
            DuckDbValueKind.HugeInt => VectorElementConverter.Create(&ConvertToNullableDecimalFromVector<Int128>),
            _ => throw new InvalidOperationException("Cannot decode Decimal from a DuckDB vector with the given storage type. ")
        };

    #endregion
}
