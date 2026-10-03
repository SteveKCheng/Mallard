using System;
using System.Runtime.CompilerServices;
using Mallard.Interop;
using Mallard.Types;

namespace Mallard;

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the writable contents of a DuckDB vector (being constructed for appending) as a .NET span.
    /// </summary>
    /// <typeparam name="T">
    /// The type of element in the DuckDB vector.  Like the read-side <c>AsSpan</c>, this method is
    /// only available for "primitive" types of fixed size.  Writing variable-length data requires
    /// cooperation from DuckDB for memory allocation and cannot be done through plain spans.
    /// This restriction gets enforced at compile-time as "ref struct" types cannot be 
    /// substituted for <typeparamref name="T" />.
    /// </typeparam>
    /// <param name="vector">The DuckDB vector to write to. </param>
    /// <returns>
    /// A writable span covering the full capacity of the DuckDB vector (<see cref="DuckDbVectorRawWriter{T}.Capacity" />
    /// elements).  Only the prefix of the span that corresponds to the number of rows reported by the
    /// chunk-writing function will actually be appended to the table.
    /// </returns>
    public static unsafe Span<T> AsSpan<T>(in this DuckDbVectorRawWriter<T> vector) where T : unmanaged
        => new(vector._info.DataPointer, vector._info.Length);

    /// <summary>
    /// Set a string as an element in a DuckDB <c>VARCHAR</c> vector.
    /// </summary>
    /// <param name="vector">The DuckDB vector to write to. </param>
    /// <param name="index">The index of the element of the vector. </param>
    /// <param name="value">The string to set into the vector's element. </param>
    /// <remarks>
    /// This is a convenience overload equivalent to
    /// <see cref="SetStringUtf16(in DuckDbVectorRawWriter{DuckDbString}, int, ReadOnlySpan{char})" />
    /// applied to <paramref name="value" />.
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="index" /> is out of range for the vector.
    /// </exception>
    public static void Set(in this DuckDbVectorRawWriter<DuckDbString> vector, int index, string value)
        => vector.SetStringUtf16(index, value.AsSpan());

    /// <summary>
    /// Set a UTF-16-encoded string as an element in a DuckDB <c>VARCHAR</c> vector.
    /// </summary>
    /// <param name="vector">The DuckDB vector to write to. </param>
    /// <param name="index">The index of the element of the vector. </param>
    /// <param name="value">The string, encoded in UTF-16 (the native .NET encoding). </param>
    /// <remarks>
    /// <para>
    /// Unlike the fixed-width types written through <see cref="DuckDbVectorRawWriter{T}.SetItem" />,
    /// a <c>VARCHAR</c> element cannot be written by .NET directly into the DuckDB vector's memory:
    /// DuckDB owns the storage for the variable-length data and must copy the bytes in itself.
    /// The characters are transcoded to UTF-8 first, and then
    /// handed to <see cref="SetStringUtf8(in DuckDbVectorRawWriter{DuckDbString}, int, ReadOnlySpan{byte})" />.
    /// </para>
    /// <para>
    /// If the element at <paramref name="index" /> was previously marked invalid via
    /// <see cref="DuckDbVectorRawWriter{T}.SetInvalid" />, it becomes valid after this call.
    /// </para>
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="index" /> is out of range for the vector.
    /// </exception>
    [SkipLocalsInit]
    public static unsafe void SetStringUtf16(in this DuckDbVectorRawWriter<DuckDbString> vector,
                                             int index,
                                             ReadOnlySpan<char> value)
    {
        using scoped var marshalState = new Utf8StringConverterState();
        var utf8Ptr = marshalState.ConvertToUtf8(value, out var utf8Length,
                                                 stackalloc byte[Utf8StringConverterState.SuggestedBufferSize]);
        vector.SetStringUtf8(index, new ReadOnlySpan<byte>(utf8Ptr, utf8Length));
    }

    /// <summary>
    /// Set a UTF-8-encoded string as an element in a DuckDB <c>VARCHAR</c> vector.
    /// </summary>
    /// <param name="vector">The DuckDB vector to write to. </param>
    /// <param name="index">The index of the element of the vector. </param>
    /// <param name="value">The string, encoded in UTF-8. </param>
    /// <remarks>
    /// <para>
    /// DuckDB validates the bytes as UTF-8 and copies them into storage it manages for the vector, so the
    /// caller does not need to keep <paramref name="value" /> alive afterward.
    /// </para>
    /// <para>
    /// If the element at <paramref name="index" /> was previously marked invalid via
    /// <see cref="DuckDbVectorRawWriter{T}.SetInvalid" />, it becomes valid after this call.
    /// </para>
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="index" /> is out of range for the vector.
    /// </exception>
    public static void SetStringUtf8(in this DuckDbVectorRawWriter<DuckDbString> vector,
                                     int index,
                                     ReadOnlySpan<byte> value)
        => AssignVariableLengthElement(vector, index, value);

    /// <summary>
    /// Set binary data as an element in a DuckDB <c>BLOB</c> vector.
    /// </summary>
    /// <param name="vector">The DuckDB vector to write to. </param>
    /// <param name="index">The index of the element of the vector. </param>
    /// <param name="data">The binary data to set.
    /// Note that byte arrays (<c>byte[]</c>) implicitly convert to spans in C# for calling this method.
    /// </param>
    /// <remarks>
    /// <para>
    /// As with strings, DuckDB owns the storage for the blob and copies the bytes in itself, so the caller
    /// does not need to keep <paramref name="data" /> alive afterward.  No encoding or validation is applied.
    /// </para>
    /// <para>
    /// If the element at <paramref name="index" /> was previously marked invalid via
    /// <see cref="DuckDbVectorRawWriter{T}.SetInvalid" />, it becomes valid after this call.
    /// </para>
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="index" /> is out of range for the vector.
    /// </exception>
    public static void SetBlob(in this DuckDbVectorRawWriter<DuckDbBlob> vector,
                               int index,
                               ReadOnlySpan<byte> data)
        => AssignVariableLengthElement(vector, index, data);

    /// <summary>
    /// Common implementation for writing a variable-length (string or blob) element: both are stored as
    /// DuckDB's <c>string_t</c> and assigned through the same native call, which copies the bytes into
    /// DuckDB-managed storage.
    /// </summary>
    private static unsafe void AssignVariableLengthElement<T>(in DuckDbVectorRawWriter<T> vector,
                                                              int index,
                                                              ReadOnlySpan<byte> bytes)
        where T : unmanaged, allows ref struct
    {
        if (unchecked((uint)index >= (uint)vector._info.Length))
            throw new IndexOutOfRangeException("Index is out of range for the vector. ");

        fixed (byte* p = bytes)
            NativeMethods.duckdb_vector_assign_string_element_len(vector._info.NativeVector, index, p, bytes.Length);

        vector.UnsafeSetValid(index);
    }
}
