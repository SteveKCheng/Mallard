using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// Represents a blob in DuckDB, as an element in some vector.
/// </summary>
/// <remarks>
/// <para>
/// This type is only used for reading elements from a DuckDB vector.  
/// (In DuckDB's C API, it corresponds to <c>duckdb_string_t</c>.)
/// It is a "ref struct"
/// as it may internally point to natively-allocated memory, which must be scoped to the
/// lifetime of the vector (<see cref="DuckDbVectorReader{T}" />).
/// </para>
/// <para>Semantically, this structure
/// is nothing more than <see cref="ReadOnlySpan{byte}" /> on the blob data,
/// which can be accessed through the property <see cref="Span" />.
/// DuckDB's representation of blobs
/// is obviously different from <see cref="ReadOnlySpan{byte}" /> so that type cannot be used
/// directly in <see cref="DuckDbVectorReader{T}" /> to read vector elements.  
/// </para>
/// <para>
/// This type is not used for sending values from .NET to DuckDB, since DuckDB needs to allocate
/// and manage the memory blocks used to hold variable-length data, and such operations cannot
/// be surfaced safely using .NET structures alone.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Explicit)]
public unsafe ref struct DuckDbBlob : IStatelesslyConvertible<DuckDbBlob, byte[]>
{
    /// <summary>
    /// Capacity of the inline buffer for short blobs.
    /// </summary>
    private const int InlinedSize = 12;

    /// <summary>
    /// Length of the blob in bytes.
    /// </summary>
    [FieldOffset(0)]
    private readonly uint _length;

    /// <summary>
    /// Inline buffer for short blobs, up to the inlined size.
    /// </summary>
    /// <remarks>
    /// This member shares space with <see cref="_ptr" />.  It is valid
    /// when <see cref="_length" /> is less than oe equal <see cref="InlinedSize" />.
    /// </remarks>
    [FieldOffset(4)]
    private fixed byte _inlined[InlinedSize];   // would be readonly if C# allowed that for fixed fields

    /// <summary>
    /// Pointer to the blob if it exceeds the inlined size.
    /// </summary>
    /// <remarks>
    /// This member shares space with <see cref="_inline" />.  It is valid
    /// when <see cref="_length" /> is greater than <see cref="InlinedSize" />.
    /// </remarks>
    [FieldOffset(8)]
    private readonly byte* _ptr;

    /// <summary>
    /// The content of the blob.
    /// </summary>
    /// <remarks>
    /// <para>
    /// There is an inlined buffer within <see cref="DuckDbBlob" /> for short blobs which the 
    /// returned span may point to.
    /// So this instance must outlive the span, enforced by this property being marked to be
    /// an "unscoped reference".
    /// </para>
    /// </remarks>
    [UnscopedRef]
    public readonly ReadOnlySpan<byte> Span
    {
        get
        {
            var length = checked((int)_length);

            // We used to use pointers in computing the first argument, which depended
            // on the fact that this structure is a "ref struct" and hence cannot ever move
            // in memory.  But to be defensive (in case this code is copied and pasted
            // somewhere else outside of a "ref struct"), we now use managed references.
            return MemoryMarshal.CreateReadOnlySpan(
                in (length <= InlinedSize) ? ref _inlined[0]
                                           : ref Unsafe.AsRef<byte>(_ptr),
                length);
        }
    }

    #region Vector element converter

    static byte[] IStatelesslyConvertible<DuckDbBlob, byte[]>.Convert(ref readonly DuckDbBlob item)
        => item.Span.ToArray();

    #endregion
}
