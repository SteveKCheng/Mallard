using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Mallard;

/// <summary>
/// DuckDB's (RFC 4122) encoding of a UUID (Universally Unique Identifier) as a 128-bit integer.
/// </summary>
/// <remarks>
/// <para>
/// The information content this type represents is the same as the standard 
/// <see cref="Guid" /> type used in most of .NET,
/// but in a different internal format.  This type offers methods to convert
/// between the formats.  
/// </para>
/// <para>
/// For clarity, we describe the formats involved:
/// <list type="bullet">
/// <item>
/// The canonical representation of a UUID, for the purposes here, 
/// shall be considered to be the
/// string consisting of 32 hexadecimal digits, which is usually
/// interpreted by humans as a big-endian 128-bit integer.
/// </item>
/// <item>
/// The DuckDB encoding splits the UUID, as a string, in the middle, 
/// into two groups of 16 hexadecimal digits.  Each group is separately
/// interpreted as a 64-bit integer in big-endian notation.  Then
/// the first group is set as the upper 64-bit word of <see cref="UInt128" />;
/// the second group is set as the lower 64-bit word of <see cref="UInt128" />.
/// Thus, the UUID is essentially read as a 128-bit integer in big-endian encoding.
/// (See <c>source/common/types/uuid.cpp</c> in DuckDB's source code.)
/// </item>
/// <item>
/// The Microsoft GUID encoding is a mixed-endian encoding.
/// For a UUID of the form <c>aa aa aa aa - xx xx - yy yy - zz zz - bb bb bb bb bb bb</c>
/// (spaces and hyphens added for clarity), the first 3 groups of hexadecimal digits,
/// namely, 
/// <c>aa aa aa aa</c>, <c>xx xx</c> and <c>yy yy</c>, are individually encoded
/// as little-endian 32-bit, 16-bit and 16-bit integers, respectively.
/// The last 2 groups of hexadecimal digits are individually encoded as 
/// big-endian 16-bit and 48-bit integers respectively.
/// </item>
/// <item>
/// The RFC 4122 format for a UUID splits the 32 hexadecimal digits into
/// 6 fields of the following lengths in bits: 32, 16, 16, 16, 48
/// separated by hyphen.  (This looks the same as Microsoft's GUID when formatted
/// as a string.)  Each group is interpreted as a big-endian integer.
/// (Since there is no mixed byte-ordering, the whole UUID could be 
/// interpreted as a 128-bit big-endian integer.  Thus the bit representation
/// of a UUID in RFC 4122 is identical to DuckDB's.)
/// </item>
/// </list>
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDbUuid(UInt128 value) : ISpanFormattable, IUtf8SpanFormattable
{
    private readonly DuckDbHugeUInt _data = new(value);

    public Guid ToGuid()
    {
        var (group1, group2, group3, lower) = SplitInto4Groups(_data);
        unchecked
        {
            return new Guid(group1, group2, group3,
                            (byte)(lower >> 56),
                            (byte)(lower >> 48),
                            (byte)(lower >> 40),
                            (byte)(lower >> 32),
                            (byte)(lower >> 24),
                            (byte)(lower >> 16),
                            (byte)(lower >> 8),
                            (byte)lower);
        }
    }

    public static DuckDbUuid FromGuid(in Guid guid)
    {
        // Writes the first 3 groups as little-endian, then the rest in big-endian.
        // See src/System/Guid.cs in System.Private.CorLib.
        Span<byte> buffer = stackalloc byte[16];
        bool success = guid.TryWriteBytes(buffer);
        Debug.Assert(success);

        var group1 = BinaryPrimitives.ReadUInt32LittleEndian(buffer[0..4]);
        var group2 = BinaryPrimitives.ReadUInt16LittleEndian(buffer[4..6]);
        var group3 = BinaryPrimitives.ReadUInt16LittleEndian(buffer[6..8]);
        var lower = BinaryPrimitives.ReadUInt64BigEndian(buffer[8..]);
        var upper = ((ulong)group1 << 32) | ((ulong)group2 << 16) | ((ulong)group3);

        return new DuckDbUuid(new UInt128(upper, lower));
    }

    /*
    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        Span<char> buffer = stackalloc char[36];
        TryFormat(buffer, out _, format, formatProvider);
        return buffer.ToString();
    }
    */

    private static (uint, ushort, ushort, ulong) SplitInto4Groups(DuckDbHugeUInt data)
    {
        unchecked
        {
            // Split "upper" word into 3 groups
            var group1 = (uint)(data.upper >> 32);
            var group2 = (ushort)(data.upper >> 16);
            var group3 = (ushort)data.upper;

            return (group1, group2, group3, data.lower);
        }
    }

    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format)
    {
        /*
        // FIXME "format" argument ignored for now
        // Uses hyphens to separate groups, but no spaces or braces

        if (destination.Length < 36)
        {
            charsWritten = 0;
            return false;
        }

        var (group1, group2, group3, lower) = SplitInto4Groups(_data);

        // Split "lower" word into 2 groups
        var group4 = (ushort)(lower >> 48);
        var group5 = lower & 0xFFFF_FFFF_FFFFul;

        group1.TryFormat(destination[0..8], out _, "X8");
        group2.TryFormat(destination[9..13], out _, "X4");
        group3.TryFormat(destination[14..18], out _, "X4");
        group4.TryFormat(destination[19..23], out _, "X4");
        group5.TryFormat(destination[24..36], out _, "X12");

        destination[8] = '-';
        destination[13] = '-';
        destination[18] = '-';
        destination[23] = '-';

        charsWritten = 36;
        return true;
        */

        return ToGuid().TryFormat(destination, out charsWritten, format);
    }

    public override string ToString() => ToString(null, null);

    public override int GetHashCode() => _data.GetHashCode();

    bool ISpanFormattable.TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
        => TryFormat(destination, out charsWritten, format);

    bool IUtf8SpanFormattable.TryFormat(Span<byte> utf8Destination, out int bytesWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
        => TryFormat(utf8Destination, out bytesWritten, format);

    public string ToString(string? format, IFormatProvider? formatProvider)
        => ToGuid().ToString(format, formatProvider);

    public bool TryFormat(Span<byte> utf8Destination, out int bytesWritten, ReadOnlySpan<char> format)
        => ToGuid().TryFormat(utf8Destination, out bytesWritten, format);

    #region Type conversions for vector reader

    private static Guid ConvertToGuidFromVector(object? state, in DuckDbVectorInfo vector, int index)
        => vector.UnsafeRead<DuckDbUuid>(index).ToGuid();

    private static object ConvertToBoxedGuidFromVector(object? state, in DuckDbVectorInfo vector, int index)
        => (object)ConvertToGuidFromVector(state, vector, index);

    private static Guid? ConvertToNullableGuidFromVector(object? state, in DuckDbVectorInfo vector, int index)
        => new Nullable<Guid>(ConvertToGuidFromVector(state, vector, index));

    internal unsafe static VectorElementConverter GetVectorElementConverter()
        => VectorElementConverter.Create(&ConvertToGuidFromVector);

    internal unsafe static VectorElementConverter GetBoxedVectorElementConverter()
        => VectorElementConverter.Create(&ConvertToBoxedGuidFromVector);

    internal unsafe static VectorElementConverter GetNullableVectorElementConverter()
        => VectorElementConverter.Create(&ConvertToNullableGuidFromVector);

    #endregion

}
