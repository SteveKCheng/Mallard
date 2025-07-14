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

    /// <summary>
    /// Convert to a .NET string.
    /// </summary>
    public readonly override string ToString() => Encoding.UTF8.GetString(DangerousGetSpan());
}
