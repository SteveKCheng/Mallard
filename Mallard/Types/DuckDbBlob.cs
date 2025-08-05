using System;
using System.Collections;
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
/// which can be accessed through the extension method <see cref="DuckDbVectorMethods.AsSpan(ref Mallard.DuckDbBlob)" />.
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
    /// Get the content of the blob.
    /// </summary>
    /// <remarks>
    /// This method is deliberately not an instance method, to disallow calling it on rvalues.
    /// Pointers to the inline buffer would become dangling when the originating rvalue
    /// disappears.
    /// </remarks>
    internal static ReadOnlySpan<byte> AsSpan(ref readonly DuckDbBlob blob)
    {
        void* p = (blob._length <= InlinedSize)
                ? Unsafe.AsPointer(ref Unsafe.AsRef(in blob._inlined[0]))
                : blob._ptr;

        return new ReadOnlySpan<byte>(p, checked((int)blob._length));
    }

    #region Vector element converter

    static byte[] IStatelesslyConvertible<DuckDbBlob, byte[]>.Convert(ref readonly DuckDbBlob item)
        => item.AsSpan().ToArray();

    #endregion
}

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the content of the blob.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This functionality exists as an extension method rather than an instance method 
    /// of <see cref="DuckDbBlob" /> only so that the "ref readonly" qualifier can be
    /// applied to the receiver argument in C#.
    /// </para>
    /// </remarks>
    /// <param name="blob">
    /// The native DuckDB structure representing the blob.
    /// The "ref readonly" qualifier is only to ensure that the argument is an lvalue,
    /// so that the returned span does not point to a rvalue that might disappear
    /// (go out of scope) before the span does.  There is an inlined buffer
    /// within <see cref="DuckDbBlob" /> for short blobs which the span may point to.
    /// </param>
    public static ReadOnlySpan<byte> AsSpan(ref readonly this DuckDbBlob blob)
        => DuckDbBlob.AsSpan(in blob);
}
