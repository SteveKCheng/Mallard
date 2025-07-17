using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Mallard;

/// <summary>
/// Represents a UTF-8 string, bit string or blob instantiated by DuckDB, as an element in some vector.
/// </summary>
/// <remarks>
/// <para>
/// This type is only used for reading elements from a DuckDB vector.  It is a "ref struct"
/// as it may internally point to natively-allocated memory, which must be scoped to the
/// lifetime of the vector (<see cref="DuckDbReadOnlyVector{T}" />).  
/// </para>
/// <para>Semantically, this structure
/// is nothing more than <see cref="ReadOnlySpan{byte}" /> on the string or blob data,
/// which can be accessed through the extension method <see cref="DuckDbReadOnlyVectorMethods.AsSpan(ref Mallard.DuckDbString)" />.
/// DuckDB's representation of strings and blobs
/// is obviously different from <see cref="ReadOnlySpan{byte}" /> so that type cannot be used
/// directly in <see cref="DuckDbReadOnlyVector{T}" /> to read vector elements.  
/// </para>
/// <para>
/// This type is not used for sending values from .NET to DuckDB, since DuckDB needs to allocate
/// and manage the memory blocks used to hold variable-length data, and such operations cannot
/// be surfaced safely using .NET structures alone.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Explicit)]
public unsafe ref struct DuckDbString
{
    /// <summary>
    /// Capacity of the inline buffer for short strings.
    /// </summary>
    private const int InlinedSize = 12;

    /// <summary>
    /// Length of the string or blob in bytes.
    /// </summary>
    [FieldOffset(0)]
    private readonly uint _length;

    /// <summary>
    /// Inline buffer for short strings, up to the inlined size.
    /// </summary>
    /// <remarks>
    /// This member shares space with <see cref="_ptr" />.  It is valid
    /// when <see cref="_length" /> is less than oe equal <see cref="InlinedSize" />.
    /// </remarks>
    [FieldOffset(4)]
    private fixed byte _inlined[InlinedSize];   // would be readonly if C# allowed that for fixed fields

    /// <summary>
    /// Pointer to the string or blob if it exceeds the inlined size.
    /// </summary>
    /// <remarks>
    /// This member shares space with <see cref="_inline" />.  It is valid
    /// when <see cref="_length" /> is greater than <see cref="InlinedSize" />.
    /// </remarks>
    [FieldOffset(8)]
    private readonly byte* _ptr;

    /// <summary>
    /// Get the span of bytes representing the UTF-8 string, bit string or blob.
    /// </summary>
    /// <remarks>
    /// This method is deliberately not an instance method, to disallow calling it on rvalues.
    /// Pointers to the inline buffer would become dangling when the originating rvalue
    /// disappears.
    /// </remarks>
    internal static ReadOnlySpan<byte> AsSpan(ref DuckDbString nativeString)
    {
        void* p = (nativeString._length <= InlinedSize) 
                    ? Unsafe.AsPointer(ref nativeString._inlined[0]) 
                    : nativeString._ptr;
        return new ReadOnlySpan<byte>(p, checked((int)nativeString._length));
    }
}

public unsafe static partial class DuckDbReadOnlyVectorMethods
{
    /// <summary>
    /// Get a string, after converting from its original UTF-8 representation, from a DuckDB vector of strings,
    /// at the specified index.
    /// </summary>
    /// <param name="vector">The vector of strings. </param>
    /// <param name="index">Which string to select from the vector. </param>
    /// <returns>
    /// The desired string.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public static string GetItem(in this DuckDbReadOnlyVector<string> vector, int index)
    {
        return Encoding.UTF8.GetString(GetStringAsUtf8(vector, index));
    }

    /// <summary>
    /// Get a string, in UTF-8 encoding, from a DuckDB vector of strings,
    /// at the specified index.
    /// </summary>
    /// <param name="vector">The vector of strings. </param>
    /// <param name="index">Which string to select from the vector. </param>
    /// <returns>
    /// The desired string.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public static ReadOnlySpan<byte> GetStringAsUtf8(in this DuckDbReadOnlyVector<string> vector, int index)
    {
        vector.VerifyItemIsValid(index);
        var p = (DuckDbString*)vector._nativeData + index;
        return p->AsSpan();
    }

    /// <summary>
    /// Get the span of bytes representing a UTF-8 string, bit string, or blob.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This functionality exists as an extension method rather than an instance method 
    /// of <see cref="DuckDbString" /> only so that the "ref" qualifier can be
    /// applied to the argument <paramref name="nativeString" /> in C#.
    /// </para>
    /// </remarks>
    /// <param name="nativeString">
    /// The native DuckDB structure representing the string or blob.
    /// The "ref" qualifier is only to ensure that the argument is an lvalue,
    /// so that the returned span does not point to a rvalue that might disappear
    /// (go out of scope) before the span does.  This method does
    /// not actually modify the argument.
    /// </param>
    public static ReadOnlySpan<byte> AsSpan(ref this DuckDbString nativeString) 
        => DuckDbString.AsSpan(ref nativeString);
}
