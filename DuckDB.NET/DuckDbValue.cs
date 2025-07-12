using DuckDB.C_API;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.Marshalling;
using System.Text;
using System.Threading.Tasks;

namespace DuckDB;

public unsafe class DuckDbValue
{
    internal static _duckdb_value* CreateNativeObject<T>(T input)
    {
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
        if (typeof(T) == typeof(float))
            return NativeMethods.duckdb_create_float((float)(object)input!);
        if (typeof(T) == typeof(double))
            return NativeMethods.duckdb_create_double((double)(object)input!);
        if (typeof(T) == typeof(bool))
            return NativeMethods.duckdb_create_bool((bool)(object)input!);

        if (typeof(T) == typeof(string))
        {
            using scoped var marshalState = new Utf8StringConverterState();
            var utf8Ptr = marshalState.ConvertToUtf8(
                (string)(object)input!,
                out int utf8Length,
                stackalloc byte[Utf8StringConverterState.SuggestedBufferSize]);
            return NativeMethods.duckdb_create_varchar_length(utf8Ptr, utf8Length);
        }

        throw new InvalidOperationException("Unsupported type. ");
    }
}
