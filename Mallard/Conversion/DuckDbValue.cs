using Mallard.Interop;
using Mallard.Types;
using System;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Mallard;

public unsafe static class DuckDbValue
{
    [SkipLocalsInit]
    private static _duckdb_value* CreateNativeString(string input)
    {
        using scoped var marshalState = new Utf8StringConverterState();
        var utf8Ptr = marshalState.ConvertToUtf8(
            input,
            out int utf8Length,
            stackalloc byte[Utf8StringConverterState.SuggestedBufferSize]);
        return NativeMethods.duckdb_create_varchar_length(utf8Ptr, utf8Length);
    }

    private static _duckdb_value* CreateBlob(ReadOnlySpan<byte> input)
    {
        fixed (byte* p = input)
            return NativeMethods.duckdb_create_blob(p, input.Length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static _duckdb_value* CreateNativeObject(object? input)
    {
        if (input is null)
            return NativeMethods.duckdb_create_null_value();
        
        if (input is bool b)
            return NativeMethods.duckdb_create_bool(b);

        if (input is sbyte i8)
            return NativeMethods.duckdb_create_int8(i8);
        if (input is byte u8)
            return NativeMethods.duckdb_create_uint8(u8);

        if (input is short i16)
            return NativeMethods.duckdb_create_int16(i16);
        if (input is ushort u16)
            return NativeMethods.duckdb_create_uint16(u16);

        if (input is int i32)
            return NativeMethods.duckdb_create_int32(i32);
        if (input is uint u32)
            return NativeMethods.duckdb_create_uint32(u32);

        if (input is long i64)
            return NativeMethods.duckdb_create_int64(i64);
        if (input is ulong u64)
            return NativeMethods.duckdb_create_uint64(u64);

        if (input is Int128 i128)
            return NativeMethods.duckdb_create_hugeint(i128);
        if (input is UInt128 u128)
            return NativeMethods.duckdb_create_uhugeint(u128);

        if (input is float f32)
            return NativeMethods.duckdb_create_float(f32);
        if (input is double f64)
            return NativeMethods.duckdb_create_double(f64);

        if (input is DuckDbDecimal dec2)
            return NativeMethods.duckdb_create_decimal(dec2);
        if (input is Decimal dec)
            return NativeMethods.duckdb_create_decimal(DuckDbDecimal.FromDecimal(dec));

        if (input is DuckDbDate date2)
            return NativeMethods.duckdb_create_date(date2);

        if (input is DuckDbTimestamp timestamp)
            return NativeMethods.duckdb_create_timestamp(timestamp);

        if (input is DateTime dateTime)
            return NativeMethods.duckdb_create_timestamp(
                DuckDbTimestamp.FromDateTime(dateTime));

        if (input is DuckDbInterval interval)
            return NativeMethods.duckdb_create_interval(interval);

        // N.B. uses a P/Invoke custom marshaller
        if (input is BigInteger big)
            return NativeMethods.duckdb_create_varint(big);

        if (input is string s)
            return CreateNativeString(s);

        if (input is byte[] blob)
            return CreateBlob(blob);

        throw new NotSupportedException(
            $"Cannot convert the given type to a DuckDB value.  Type: {input.GetType().Name}");
    }


    internal static _duckdb_value* CreateNativeObject<T>(T input)
        => CreateNativeObject((object?)input);
    
    /// <summary>
    /// Set a boolean value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, bool value) where TReceiver : ISettableDuckDbValue
        => receiver.SetBoolean(value);
    
    /// <summary>
    /// Set a 32-bit signed integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, int value) where TReceiver : ISettableDuckDbValue
        => receiver.SetInt32(value);
    
    /// <summary>
    /// Set a 64-bit signed integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, long value) where TReceiver : ISettableDuckDbValue
        => receiver.SetInt64(value);
    
    /// <summary>
    /// Set an 8-bit signed integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, sbyte value) where TReceiver : ISettableDuckDbValue
        => receiver.SetInt8(value);
    
    /// <summary>
    /// Set a 16-bit signed integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, short value) where TReceiver : ISettableDuckDbValue
        => receiver.SetInt16(value);
    
    /// <summary>
    /// Set a 128-bit signed integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, Int128 value) where TReceiver : ISettableDuckDbValue
        => receiver.SetInt128(value);
    
    /// <summary>
    /// Set an 8-bit unsigned integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, byte value) where TReceiver : ISettableDuckDbValue
        => receiver.SetUInt8(value);
    
    /// <summary>
    /// Set a 16-bit unsigned integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, ushort value) where TReceiver : ISettableDuckDbValue
        => receiver.SetUInt16(value);
    
    /// <summary>
    /// Set a 32-bit unsigned integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, uint value) where TReceiver : ISettableDuckDbValue
        => receiver.SetUInt32(value);
    
    /// <summary>
    /// Set a 64-bit unsigned integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, ulong value) where TReceiver : ISettableDuckDbValue
        => receiver.SetUInt64(value);
    
    /// <summary>
    /// Set a 128-bit unsigned integer value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, UInt128 value) where TReceiver : ISettableDuckDbValue
        => receiver.SetUInt128(value);
    
    /// <summary>
    /// Set a single-precision floating-point value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, float value) where TReceiver : ISettableDuckDbValue
        => receiver.SetFloat(value);
    
    /// <summary>
    /// Set a double-precision floating-point value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, double value) where TReceiver : ISettableDuckDbValue
        => receiver.SetDouble(value);

    /// <summary>
    /// Set a decimal value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, DuckDbDecimal value) where TReceiver : ISettableDuckDbValue
        => receiver.SetDecimal(value);

    /// <summary>
    /// Set a string value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The UTF-8 encoded bytes to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, string value) where TReceiver : ISettableDuckDbValue
        => receiver.SetStringUtf16(value.AsSpan());
    
    /// <summary>
    /// Set a UTF-16-encoded string value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The string encoded in UTF-16. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    [SkipLocalsInit]
    public static void SetStringUtf16<TReceiver>(this TReceiver receiver, ReadOnlySpan<char> value)
        where TReceiver : ISettableDuckDbValue
    {
        using scoped var marshalState = new Utf8StringConverterState();
        var utf8Ptr = marshalState.ConvertToUtf8(
            value,
            out int utf8Length,
            stackalloc byte[Utf8StringConverterState.SuggestedBufferSize]);
        receiver.SetStringUtf8(new ReadOnlySpan<byte>(utf8Ptr, utf8Length));
    }

    /// <summary>
    /// Set a UTF-8-encoded string value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The string encoded in UTF-8. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void SetStringUtf8<TReceiver>(this TReceiver receiver, ReadOnlySpan<byte> value) where TReceiver : ISettableDuckDbValue
        => receiver.SetStringUtf8(value);
    
    /// <summary>
    /// Set a blob value into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="value">The binary data to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    public static void Set<TReceiver>(this TReceiver receiver, ReadOnlySpan<byte> value) where TReceiver : ISettableDuckDbValue
        => receiver.SetBlob(value);
}
