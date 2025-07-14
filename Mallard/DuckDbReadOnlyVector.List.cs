using Mallard.C_API;
using System;
using System.Runtime.InteropServices;

namespace Mallard;

public unsafe static partial class DuckDbReadOnlyVectorMethods
{
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
    public static DuckDbReadOnlyVector<T> GetChildrenVector<T>(in this DuckDbReadOnlyVector<DuckDbList> parent)
    {
        var parentVector = parent._nativeVector;
        ThrowOnNullVector(parentVector);

        var childVector = NativeMethods.duckdb_list_vector_get_child(parentVector);
        if (childVector == null)
            throw new DuckDbException("Could not get the child vector from a list vector in DuckDB. ");

        var totalChildren = NativeMethods.duckdb_list_vector_get_size(parentVector);

        var childBasicType = GetVectorElementBasicType(childVector);
        ThrowOnWrongClrType<T>(childBasicType);

        return new DuckDbReadOnlyVector<T>(childVector, childBasicType, (int)totalChildren);
    }

    public static ReadOnlySpan<DuckDbListChild> GetChildrenSpan(in this DuckDbReadOnlyVector<DuckDbList> parent)
    {
        return new ReadOnlySpan<DuckDbListChild>(parent._nativeData, parent._length);
    }

    /// <summary>
    /// Get the range of indices in the children vector of a vector of lists that
    /// hold the children of a particular list at the specified index.
    /// </summary>
    /// <param name="parent">The vector of lists. </param>
    /// <param name="index">The list-valued element in <paramref name="parent" /> to select. </param>
    /// <returns>
    /// The range of indices in the children vector as returned by 
    /// <see cref="GetChildrenVector{T}" /> applied to <paramref name="parent" />.
    /// </returns>
    public static Range GetChildrenFor(in this DuckDbReadOnlyVector<DuckDbList> parent, int index)
    {
        parent.VerifyItemIsValid(index);
        var c = parent.GetChildrenSpan()[index];
        return new Range((int)c.Offset, (int)c.Offset + (int)c.Length);
    }
}

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDbListChild
{
    public readonly ulong Offset;
    public readonly ulong Length;
}
