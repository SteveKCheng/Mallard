using Mallard.C_API;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Mallard;

/// <summary>
/// Represents a string instantiated by DuckDB as an element in some vector.
/// </summary>
/// <remarks>
/// <para>
/// Due to this type having both an inline buffer and a pointer, there is no way to expose
/// it safely to user code (without allowing the possibility of dangling pointers).
/// This type is only used internally to translate the data to a more standard form:
/// either <see cref="ReadOnlySpan{byte}" /> (UTF-8) or <see cref="string" />.
/// </para>
/// <para>
/// Naturally, C# code, even within this library, cannot modify the string through this
/// structure.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Explicit)]
internal unsafe ref struct DuckDbReadOnlyString
{
    /// <summary>
    /// Capacity of the inline buffer for short strings.
    /// </summary>
    private const int InlinedSize = 12;

    /// <summary>
    /// Length of the UTF-8 string in bytes.
    /// </summary>
    [FieldOffset(0)]
    private readonly uint _length;

    /// <summary>
    /// Inline buffer for short strings, up to the inlined size.
    /// </summary>
    [FieldOffset(4)]
    private fixed byte _inlined[InlinedSize];

    /// <summary>
    /// Pointer to the UTF-8 string data if it exceeds the inlined size.
    /// </summary>
    [FieldOffset(8)]
    private readonly byte* _ptr;

    /// <summary>
    /// Get the span of UTF-8 bytes representing the string.
    /// </summary>
    /// <remarks>
    /// Caution: do not call this method on an rvalue, for in that case, any pointer to the inline buffer
    /// would not be valid after this method returns.
    /// </remarks>
    public readonly ReadOnlySpan<byte> DangerousGetSpan()
    {
        void* p = (_length <= InlinedSize) ? Unsafe.AsPointer(ref Unsafe.AsRef(in _inlined[0])) : _ptr;
        return new ReadOnlySpan<byte>(p, checked((int)_length));
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
        ThrowIfIndexOutOfRange(index, vector._length);
        var p = (DuckDbReadOnlyString*)vector._nativeData + index;
        return p->DangerousGetSpan();
    }
}
