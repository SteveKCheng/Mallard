using System;
using Mallard.Interop;

namespace Mallard;
using Mallard.Types;

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the raw vector for the children of a vector of lists.
    /// </summary>
    /// <param name="parent">
    /// The raw vector of lists.
    /// </param>
    /// <typeparam name="T">
    /// The element type of the lists.  It must be compatible with <see cref="DuckDbVectorRawReader{T}" />.
    /// </typeparam>
    /// <returns>
    /// The raw vector for reading the children (items) of the lists inside <paramref name="parent" />. 
    /// </returns>
    public static DuckDbVectorRawReader<T> GetChildrenRawVector<T>(in this DuckDbVectorRawReader<DuckDbArrayRef> parent)
        where T : unmanaged, allows ref struct
        => new(parent._info.GetArrayChildrenVectorInfo());

    internal unsafe static DuckDbVectorInfo GetArrayChildrenVectorInfo(in this DuckDbVectorInfo parent)
    {
        parent.ThrowIfNull();
        var parentVector = parent.NativeVector;

        var childVector = NativeMethods.duckdb_array_vector_get_child(parentVector);
        if (childVector == null)
            throw new DuckDbException("Could not get the child vector from an array vector in DuckDB. ");

        var totalChildren = checked((int)(parent.Length * parent.ColumnInfo.ElementSize));
        return new DuckDbVectorInfo(childVector, totalChildren, new DuckDbColumnInfo(childVector));
    }
}
