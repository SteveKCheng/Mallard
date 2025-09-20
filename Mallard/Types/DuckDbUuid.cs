using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Mallard.Interop;

namespace Mallard.Types;

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
public readonly struct DuckDbUuid(UInt128 value) 
    : ISpanFormattable, IUtf8SpanFormattable, IStatelesslyConvertible<DuckDbUuid, Guid>
{
    private readonly DuckDbUInt128 _data = new(value);

    /// <summary>
    /// Convert to a standard .NET GUID instance.
    /// </summary>
    /// <returns>
    /// An instance of <see cref="Guid" /> that represents the same value (as a UUID) as this instance. 
    /// </returns>
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

    /// <summary>
    /// Convert from a standard .NET GUID instance.
    /// </summary>
    /// <param name="guid">
    /// The source value to convert from.
    /// </param>
    /// <returns>
    /// An instance of <see cref="DuckDbUuid" /> that represents the same value (as a UUID)
    /// as <paramref name="guid" />.
    /// </returns>
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

    /// <summary>
    /// Break a 128-bit value, representing a UUID, into the four groups in Microsoft's
    /// interpretation as a GUID.
    /// </summary>
    private static (uint, ushort, ushort, ulong) SplitInto4Groups(DuckDbUInt128 data)
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

    /// <summary>
    /// Format this UUID into a character buffer.
    /// </summary>
    /// <param name="destination">
    /// The buffer to format into.
    /// </param>
    /// <param name="charsWritten">
    /// The number of characters written into the buffer.
    /// </param>
    /// <param name="format">
    /// Format string.  All of the options for formatting a <see cref="Guid" /> may be used.
    /// </param>
    /// <returns>
    /// True if formatting is successful.  False if the buffer is too small; the caller
    /// should retry with a larger buffer. 
    /// </returns>
    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format)
        => ToGuid().TryFormat(destination, out charsWritten, format);

    /// <inheritdoc />
    public override string ToString() => ToString(null);

    /// <inheritdoc />
    public override int GetHashCode() => _data.GetHashCode();

    bool ISpanFormattable.TryFormat(Span<char> destination,
                                    out int charsWritten, 
                                    ReadOnlySpan<char> format, 
                                    IFormatProvider? provider)
        => TryFormat(destination, out charsWritten, format);

    bool IUtf8SpanFormattable.TryFormat(Span<byte> utf8Destination, 
                                        out int bytesWritten, 
                                        ReadOnlySpan<char> format, 
                                        IFormatProvider? provider)
        => TryFormat(utf8Destination, out bytesWritten, format);

    string IFormattable.ToString(string? format, IFormatProvider? provider)
        => ToString(format);
    
    /// <summary>
    /// Format this UUID as a string.
    /// </summary>
    /// <param name="format">
    /// Format string.  All of the options for formatting a <see cref="Guid" /> may be used.
    /// </param>
    public string ToString([StringSyntax(StringSyntaxAttribute.GuidFormat)] string? format)
        => ToGuid().ToString(format);

    /// <summary>
    /// Format this UUID as a UTF-8 string.
    /// </summary>
    /// <param name="utf8Destination">
    /// The buffer to put the UTF-8 bytes into.
    /// </param>
    /// <param name="bytesWritten">
    /// The number of bytes written into the buffer.
    /// </param>
    /// <param name="format">
    /// Format string.  All of the options for formatting a <see cref="Guid" /> may be used.
    /// </param>
    /// <returns>
    /// True if formatting is successful.  False if the buffer is too small; the caller
    /// should retry with a larger buffer. 
    /// </returns>
    public bool TryFormat(Span<byte> utf8Destination, 
                          out int bytesWritten, 
                          [StringSyntax(StringSyntaxAttribute.GuidFormat)] ReadOnlySpan<char> format)
        => ToGuid().TryFormat(utf8Destination, out bytesWritten, format);

    #region Type conversions for vector reader

    static Guid IStatelesslyConvertible<DuckDbUuid, Guid>.Convert(ref readonly DuckDbUuid item) => item.ToGuid();

    #endregion
}
