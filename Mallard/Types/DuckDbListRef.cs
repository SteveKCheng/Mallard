﻿using Mallard.C_API;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard;

public static partial class DuckDbVectorMethods
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
    public static DuckDbVectorRawReader<T> GetChildrenRaw<T>(in this DuckDbVectorRawReader<DuckDbListRef> parent)
        where T : unmanaged, allows ref struct
    {
        parent._info.ThrowIfNull();
        var childVector = parent._info.GetListChildrenVectorInfo(Unsafe.NullRef<DuckDbColumnInfo>());
        return new(childVector);
    }

    /// <summary>
    /// Retrieve the vector of children, of the lists in the parent vector.
    /// </summary>
    /// <param name="parentVector">
    /// A vector with elements typed as lists in DuckDB.
    /// </param>
    /// <param name="columnInfo">
    /// Data type information for the vector of list children.  This argument may be a null reference, in which case
    /// this method will retrieve it,  The caller may supply it if it is already available, avoiding
    /// unnecessary API calls to the DuckDB native library.  
    /// </param>
    /// <returns>
    /// Description of the vector of the all lists' children.
    /// </returns>
    internal unsafe static DuckDbVectorInfo GetListChildrenVectorInfo(in this DuckDbVectorInfo parentVector, in DuckDbColumnInfo columnInfo)
    {
        var childVector = NativeMethods.duckdb_list_vector_get_child(parentVector.NativeVector);
        if (childVector == null)
            throw new DuckDbException("Could not get the child vector from a list vector in DuckDB. ");

        var totalChildren = NativeMethods.duckdb_list_vector_get_size(parentVector.NativeVector);

        return new DuckDbVectorInfo(
                    childVector, 
                    length: (int)totalChildren,
                    columnInfo: Unsafe.IsNullRef(in columnInfo) ? new DuckDbColumnInfo(childVector)
                                                                : columnInfo);      
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

