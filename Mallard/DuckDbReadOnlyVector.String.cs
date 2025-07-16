using Mallard.C_API;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Mallard;

/// <summary>
/// Represents a UTF-8 string or blob instantiated by DuckDB as an element in some vector.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
internal unsafe ref struct DuckDbReadOnlyString
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
    [FieldOffset(4)]
    private fixed byte _inlined[InlinedSize];   // would be readonly if C# allowed that for fixed fields

    /// <summary>
    /// Pointer to the string or blob if it exceeds the inlined size.
    /// </summary>
    [FieldOffset(8)]
    private readonly byte* _ptr;

    /// <summary>
    /// Get the span of bytes representing the UTF-8 string or blob.
    /// </summary>
    /// <remarks>
    /// This method is deliberately not an instance method, to disallow calling it on rvalues.
    /// Pointers to the inline buffer would become dangling when the originating rvalue
    /// disappears.
    /// </remarks>
    /// <param name="nativeString">
    /// The native DuckDB structure representing the string or blob.
    /// The "ref" qualifier is only to ensure that the argument is an lvalue; this method does
    /// not actually modify the argument.
    /// </param>
    internal static ReadOnlySpan<byte> AsSpan(ref DuckDbReadOnlyString nativeString)
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
        var p = (DuckDbReadOnlyString*)vector._nativeData + index;
        return p->AsSpan();
    }

    /// <summary>
    /// Get the span of bytes representing a UTF-8 string, or blob.
    /// </summary>
    /// <remarks>
    /// This extension method simply wraps <see cref="DuckDbReadOnlyString.AsSpan" />
    /// to enable calling it with the normal object-oriented syntax, but only for lvalues.
    /// </remarks>
    /// <param name="nativeString">
    /// The native DuckDB structure representing the string or blob.
    /// The "ref" qualifier is only to ensure that the argument is an lvalue; this method does
    /// not actually modify the argument.
    /// </param>
    internal static ReadOnlySpan<byte> AsSpan(ref this DuckDbReadOnlyString nativeString) 
        => DuckDbReadOnlyString.AsSpan(ref nativeString);
}
