using System;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the contents of a DuckDB vector as a .NET span.
    /// </summary>
    /// <typeparam name="T">
    /// The type of element in the DuckDB vector.  This method is only available for
    /// "primitive" types, i.e. types containing data of fixed size, as variable-length 
    /// data cannot be safely exposed except through a "ref struct", but a "ref struct"
    /// cannot be the element type of <see cref="ReadOnlySpan{T}" />.
    /// </typeparam>
    /// <param name="vector">The DuckDB vector to read from. </param>
    /// <returns>
    /// Span covering all the elements of the DuckDB vector.
    /// Note that elements of the vector that are invalid, may be "garbage" or un-initialized when
    /// indexed using the returned span. 
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// When <typeparamref name="T" /> is <see cref="DuckDbArrayRef" />.  
    /// A DuckDB vector of such type does not have directly have elements, and therefore
    /// no span can be made available.  The contents of vector are made available in the
    /// "children vector".
    /// </exception>
    public static unsafe ReadOnlySpan<T> AsSpan<T>(in this DuckDbVectorRawReader<T> vector) where T : unmanaged
    {
        if (typeof(T) == typeof(DuckDbArrayRef))
            ThrowForAccessingNonexistentItems(typeof(T));

        return new(vector._info.DataPointer, vector._info.Length);
    }

    [DoesNotReturn]
    internal static void ThrowForAccessingNonexistentItems(Type t)
    {
        throw new InvalidOperationException($"There are no items that can be directly accesed on DuckDbVectorRawReader<T> for T = {t.Name}. ");
    }
}
