using System;

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
}
