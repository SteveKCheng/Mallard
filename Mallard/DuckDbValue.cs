using Mallard.C_API;
using System;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Mallard;

public unsafe class DuckDbValue
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

    internal static _duckdb_value* CreateNativeObject<T>(T input)
    {
        if (typeof(T) == typeof(bool))
            return NativeMethods.duckdb_create_bool((bool)(object)input!);

        if (typeof(T) == typeof(sbyte))
            return NativeMethods.duckdb_create_int8((sbyte)(object)input!);
        if (typeof(T) == typeof(byte))
            return NativeMethods.duckdb_create_uint8((byte)(object)input!);

        if (typeof(T) == typeof(short))
            return NativeMethods.duckdb_create_int16((short)(object)input!);
        if (typeof(T) == typeof(ushort))
            return NativeMethods.duckdb_create_uint16((ushort)(object)input!);

        if (typeof(T) == typeof(int))
            return NativeMethods.duckdb_create_int32((int)(object)input!);
        if (typeof(T) == typeof(uint))
            return NativeMethods.duckdb_create_uint32((uint)(object)input!);

        if (typeof(T) == typeof(long))
            return NativeMethods.duckdb_create_int64((long)(object)input!);
        if (typeof(T) == typeof(ulong))
            return NativeMethods.duckdb_create_uint64((ulong)(object)input!);

        if (typeof(T) == typeof(Int128))
            return NativeMethods.duckdb_create_hugeint((Int128)(object)input!);
        if (typeof(T) == typeof(UInt128))
            return NativeMethods.duckdb_create_uhugeint((UInt128)(object)input!);

        if (typeof(T) == typeof(float))
            return NativeMethods.duckdb_create_float((float)(object)input!);
        if (typeof(T) == typeof(double))
            return NativeMethods.duckdb_create_double((double)(object)input!);

        if (typeof(T) == typeof(DuckDbDecimal))
            return NativeMethods.duckdb_create_decimal((DuckDbDecimal)(object)input!);
        if (typeof(T) == typeof(Decimal))
            return NativeMethods.duckdb_create_decimal(DuckDbDecimal.FromDecimal((Decimal)(object)input!));

        if (typeof(T) == typeof(DuckDbDate))
            return NativeMethods.duckdb_create_date((DuckDbDate)(object)input!);

        if (typeof(T) == typeof(DuckDbTimestamp))
            return NativeMethods.duckdb_create_timestamp((DuckDbTimestamp)(object)input!);

        if (typeof(T) == typeof(DateTime))
            return NativeMethods.duckdb_create_timestamp(
                DuckDbTimestamp.FromDateTime((DateTime)(object)input!));

        if (typeof(T) == typeof(DuckDbInterval))
            return NativeMethods.duckdb_create_interval((DuckDbInterval)(object)input!);

        // N.B. uses a P/Invoke custom marshaller
        if (typeof(T) == typeof(BigInteger))
            return NativeMethods.duckdb_create_varint((BigInteger)(object)input!);

        if (typeof(T) == typeof(string))
            return CreateNativeString((string)(object)input!);

        throw new InvalidOperationException("Unsupported type. ");
    }
}

internal static class FnPtrTest
{
    static object Generic<T>(string input) where T : INumber<T>
    {
        return (object)T.Parse(input, null);
    }

    static unsafe void Use()
    {
        delegate*<string, object> f = &Generic<int>;
       
    }
}