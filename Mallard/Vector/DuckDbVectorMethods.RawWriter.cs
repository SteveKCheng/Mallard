using System;

namespace Mallard;
using Mallard.Types;

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the writable contents of a DuckDB vector (being constructed for appending) as a .NET span.
    /// </summary>
    /// <typeparam name="T">
    /// The type of element in the DuckDB vector.  As for the read-side <c>AsSpan</c>, this method is
    /// only available for "primitive" types of fixed size, as variable-length data cannot be written
    /// through a plain span.
    /// </typeparam>
    /// <param name="vector">The DuckDB vector to write to. </param>
    /// <returns>
    /// A writable span covering the full capacity of the DuckDB vector (<see cref="DuckDbVectorRawWriter{T}.Capacity" />
    /// elements).  Only the prefix of the span that corresponds to the number of rows reported by the
    /// chunk-writing function will actually be appended to the table.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// When <typeparamref name="T" /> is <see cref="DuckDbArrayRef" /> or <see cref="DuckDbStructRef" />,
    /// which do not have a directly-writable element array.
    /// </exception>
    public static unsafe Span<T> AsSpan<T>(in this DuckDbVectorRawWriter<T> vector) where T : unmanaged
    {
        if (typeof(T) == typeof(DuckDbArrayRef) || typeof(T) == typeof(DuckDbStructRef))
            ThrowForAccessingNonexistentItems(typeof(T));

        return new(vector._info.DataPointer, vector._info.Length);
    }
}
