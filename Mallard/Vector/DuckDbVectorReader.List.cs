using Mallard.C_API;
using System;
using System.Runtime.InteropServices;

namespace Mallard;

public unsafe static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Retrieve the vector containing all the children across all lists in a vector of lists,
    /// allowing "raw" access (spans).
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind an element of the lists to.
    /// </typeparam>
    /// <param name="parent">
    /// The vector of lists.
    /// </param>
    /// <returns>
    /// The lists' children, collected into one vector, i.e. the "children vector" or "vector of list children".
    /// </returns>
    /// <exception cref="DuckDbException"></exception>
    public static DuckDbVectorRawReader<T> GetChildrenRawVector<T>(in this DuckDbVectorRawReader<DuckDbListRef> parent)
        where T : unmanaged, allows ref struct
        => new(parent.GetChildrenVectorInfo());

    /// <summary>
    /// Retrieve the vector containing all the children across all lists in a vector of lists.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind an element of the lists to.
    /// </typeparam>
    /// <param name="parent">
    /// The vector of lists.
    /// </param>
    /// <returns>
    /// The lists' children, collected into one vector, i.e. the "children vector" or "vector of list children".
    /// </returns>
    /// <exception cref="DuckDbException"></exception>
    public static DuckDbVectorReader<T> GetChildrenVector<T>(in this DuckDbVectorRawReader<DuckDbListRef> parent)
        where T : notnull
        => new(parent.GetChildrenVectorInfo());

    private static DuckDbVectorInfo GetChildrenVectorInfo(in this DuckDbVectorRawReader<DuckDbListRef> parent)
    {
        var parentVector = parent._info.NativeVector;
        ThrowOnNullVector(parentVector);

        var childVector = NativeMethods.duckdb_list_vector_get_child(parentVector);
        if (childVector == null)
            throw new DuckDbException("Could not get the child vector from a list vector in DuckDB. ");

        var totalChildren = NativeMethods.duckdb_list_vector_get_size(parentVector);
        var childBasicType = GetVectorElementBasicType(childVector);

        return new DuckDbVectorInfo(childVector, childBasicType, (int)totalChildren);
    }
}

/// <summary>
/// Reports where the data for one list resides in a list-valued DuckDB vector.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDbListRef
{
    // We do not support vectors of length > int.MaxValue
    // (not sure if this is even possible in DuckDB itself).
    // But DuckDB's C API uses uint64_t which we must mimick here.
    // We unconditionally cast it to int in the properties below
    // so user code does not have to do so.
    private readonly ulong _offset;
    private readonly ulong _length;

    /// <summary>
    /// The index of the first item of the target list, within the list vector's
    /// "children vector".
    /// </summary>
    public int Offset => unchecked((int)_offset);

    /// <summary>
    /// The length of the target list.
    /// </summary>
    public int Length => unchecked((int)_length);
}
