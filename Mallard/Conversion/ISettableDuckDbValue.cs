using System;
using System.Runtime.CompilerServices;
using Mallard.Interop;
using Mallard.Types;

namespace Mallard;

/// <summary>
/// Accepts a value from .NET code to pass into DuckDB. 
/// </summary>
/// <remarks>
/// <para>
/// There are a several kinds of objects in DuckDB that take in values that may be
/// of any of the types supported in its SQL dialect, including but not limited to
/// numbers, dates/times, strings, and blobs.  Ultimately there need to be conversions
/// from .NET values to their native representations under DuckDB.
/// </para>
/// <para>
/// (These "input" conversions are in the opposite direction of the "output" conversions
/// that happen when retrieving values/results from DuckDB.)
/// </para>
/// <para>
/// To make the supported input types easy to discover, for each supported type,
/// an extension method to set values of that type is defined in <see cref="DuckDbValue" />.
/// Thus, the static type is visible statically, instead of being erased as
/// <see cref="object" />, or hidden behind a unconstrained generic parameter
/// (although such methods are also available for the benefit of generic code).
/// This way, the compiler may also apply implicit conversions for the input types. 
/// </para>
/// <para>
/// The receiver object implements this interface, which has no public methods.
/// All the value-setting operations are implemented in <see cref="DuckDbValue" />.
/// This simplifies the implementation, makes the basic API stable, and
/// at the same time, allows users to define their own conversions for other input types
/// on almost the same footing.
/// </para>
/// </remarks>
public unsafe interface ISettableDuckDbValue
{
    /// <summary>
    /// Accept a value which has been packaged into DuckDB's generic value wrapper.
    /// </summary>
    /// <param name="nativeValue">
    /// The native object created to represent the (original) input value.
    /// This method takes ownership of it (whether this method succeeds or fails with
    /// an exception).  
    /// </param>
    internal void SetNativeValue(_duckdb_value* nativeValue);

    // The following "simple" types may have "direct" binding functions in DuckDB's C API 
    // which take in the value without having to allocate memory for _duckdb_value.
    //
    // We define an interface method for each of those functions so we may
    // preferentially use the function when it is available.  The default implementations
    // fall back to allocating the _duckdb_value wrapper and then calling SetNativeValue,
    // which works for any type supported by DuckDB.
    //
    // That DuckDB's C API works this way is considered an implementation detail for .NET
    // clients, so these methods are not part of the public API.  They are wrapped by
    // extension methods inside the static class DuckDbValue.
    //
    // Technical limitations with how default interface methods are implemented in the
    // .NET run-time mean that, in general, calling them on a struct requires that the
    // struct be boxed.  See
    // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/proposals/csharp-8.0/default-interface-methods.
    // But, fortunately, we can check from the assembly code produced by the compiler
    // that if the default interface method can be inlined, the boxing can be optimized
    // away too.  So we set our methods to be "aggressively inlined" into their callers
    // (where the struct implementing ISettableDuckDbValue will be known), and hope the
    // compiler does inline them.  (If it does not, we will have to stop using default
    // interface methods, and manually define their bodies for each implementing struct.

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetNull() => SetNativeValue(NativeMethods.duckdb_create_null_value());

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetBoolean(bool value) => SetNativeValue(NativeMethods.duckdb_create_bool(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetInt8(sbyte value) => SetNativeValue(NativeMethods.duckdb_create_int8(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetInt16(short value) => SetNativeValue(NativeMethods.duckdb_create_int16(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetInt32(int value) => SetNativeValue(NativeMethods.duckdb_create_int32(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetInt64(long value) => SetNativeValue(NativeMethods.duckdb_create_int64(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetInt128(Int128 value) => SetNativeValue(NativeMethods.duckdb_create_hugeint(value));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetUInt8(byte value) => SetNativeValue(NativeMethods.duckdb_create_uint8(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetUInt16(ushort value) => SetNativeValue(NativeMethods.duckdb_create_uint16(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetUInt32(uint value) => SetNativeValue(NativeMethods.duckdb_create_uint32(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetUInt64(ulong value) => SetNativeValue(NativeMethods.duckdb_create_uint64(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetUInt128(UInt128 value) => SetNativeValue(NativeMethods.duckdb_create_uhugeint(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetFloat(float value) => SetNativeValue(NativeMethods.duckdb_create_float(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetDouble(double value) => SetNativeValue(NativeMethods.duckdb_create_double(value));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetDecimal(DuckDbDecimal value) => SetNativeValue(NativeMethods.duckdb_create_decimal(value));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetStringUtf8(ReadOnlySpan<byte> span)
    {
        fixed (byte* p = span)
            SetNativeValue(NativeMethods.duckdb_create_varchar_length(p, span.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetBlob(ReadOnlySpan<byte> span)
    {
        fixed (byte* p = span)
            SetNativeValue(NativeMethods.duckdb_create_blob(p, span.Length));
    }
    
    // TODO: implement
    // date
    // time
    // timestamp
    // timestamp_tz
    // interval
}
