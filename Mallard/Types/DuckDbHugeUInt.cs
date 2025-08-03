using System;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// DuckDB's "huge integers": 128-bit integers.
/// </summary>
/// <remarks>
/// <para>
/// Assuming little-endianness and twos' complement representation, both DuckDB and .NET
/// decompose a 128-integer into two 64-bit integers in the natural way.  
/// Therefore we expect to
/// just use the existing UInt128 and Int128 types from .NET to read and write
/// DuckDB "huge integers".
/// </para>
/// <para>
/// Unfortunately, the .NET runtime (as of version 9) has a bug in marshalling UInt128, 
/// Int128 under P/Invoke.  It simply fails with a C++ exception thrown internally from the runtime.
/// See the discussion under <a href="https://github.com/dotnet/runtime/pull/74123">dotnet/runtime PR 74123</a>.
/// It looks like some platforms require 16-byte alignment of the structure (while DuckDB just wants 
/// a plain struct with 8-byte alignment), so this issue may persist in the future.
/// </para>
/// <para>
/// We work around the issue by defining DuckDB's structure, and 
/// "reinterpret-casting" between this structure and UInt128, Int128 as necessary.
/// </para>
/// <para>
/// Since we do not expect to do math directly on this structure, we do not bother
/// with the signed/unsigned distinction.  We cast both Int128 and UInt128 into this structure,
/// and vice versa.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct DuckDbHugeUInt
{
    public ulong lower;
    public ulong upper;

    public unsafe DuckDbHugeUInt(Int128 value)
    {
        // FIXME: Assumes little-endian
        var p = (ulong*)&value;
        lower = p[0];
        upper = p[1];
    }

    public unsafe DuckDbHugeUInt(UInt128 value)
    {
        // FIXME: Assumes little-endian
        var p = (ulong*)&value;
        lower = p[0];
        upper = p[1];
    }

    public readonly Int128 ToInt128() => new(upper, lower);
    public readonly UInt128 ToUInt128() => new(upper, lower);

    public override int GetHashCode()
        => BitConverter.IsLittleEndian ? HashCode.Combine(lower, upper) : HashCode.Combine(upper, lower);
}
