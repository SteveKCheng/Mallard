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
        => receiver.SetNativeValue(NativeMethods.duckdb_create_bool(value));
    
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
        => receiver.SetNativeValue(NativeMethods.duckdb_create_int32(value));
    
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
        => receiver.SetNativeValue(NativeMethods.duckdb_create_int64(value));
}
