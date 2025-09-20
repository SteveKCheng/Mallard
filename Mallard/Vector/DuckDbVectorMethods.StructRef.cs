using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Mallard;
using Mallard.Interop;
using Mallard.Types;

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the name of a member for a struct-typed vector.
    /// </summary>
    /// <param name="parent">
    /// The struct-typed DuckDB vector.
    /// </param>
    /// <param name="memberIndex">
    /// The index of the member in the struct, numbered from 0 (inclusive) to
    /// <see cref="DuckDbColumnInfo.ElementSize" /> (exclusive).
    /// </param>
    /// <returns>
    /// The name of the struct member.
    /// </returns>
    /// <remarks>
    /// <para>
    /// The name needs to be queried from DuckDB every time this method is called, 
    /// so the caller should cache the result if it is needed multiple times.
    /// </para>
    /// </remarks>
    public static unsafe string GetMemberName(in this DuckDbVectorRawReader<DuckDbStructRef> parent, int memberIndex)
    {
        parent._info.ThrowIfNull();
        CheckStructMemberIndex(parent._info, memberIndex);

        // Having to retrieve the logical type first to get the member name, and
        // then de-allocate it, is unfortunate, but there is nothing else we can do.
        // A raw reader is not a place to be caching column names.
        using var holder = new NativeLogicalTypeHolder(NativeMethods.duckdb_vector_get_column_type(parent._info.NativeVector));
        return NativeMethods.duckdb_struct_type_child_name(holder.NativeHandle, memberIndex);
    }

    /// <summary>
    /// Throw an exception if the member index is out of range for a struct vector.
    /// </summary>
    /// <remarks>
    /// Looking at DuckDB's source code, the C API does not check its member index input is valid,
    /// so sending an out-of-range index may be "undefined behavior".
    /// </remarks>
    private static void CheckStructMemberIndex(in this DuckDbVectorInfo vector, int memberIndex)
    {
        Debug.Assert(vector.ColumnInfo.ValueKind == DuckDbValueKind.Struct,
                     "This method should only be called for a vector of STRUCT element type. ");

        if ((uint)memberIndex >= (uint)vector.ColumnInfo.ElementSize)
        {
            throw new ArgumentOutOfRangeException(nameof(memberIndex),
                $"Member index {memberIndex} is out of range for this STRUCT type. ");
        }
    }

    /// <summary>
    /// Retrieve the vector containing all the items of one member of a struct,
    /// within a struct-typed parent vector.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind struct member values to.
    /// </typeparam>
    /// <param name="parent">
    /// The struct-typed parent vector.
    /// </param>
    /// <param name="memberIndex">
    /// The index of the member in the struct, numbered starting from 0.
    /// </param>
    /// <returns>
    /// The lists' children, collected into one vector, i.e. the "children vector" or "vector of list children".
    /// </returns>
    public static DuckDbVectorRawReader<T> GetMemberItemsRaw<T>(in this DuckDbVectorRawReader<DuckDbStructRef> parent, int memberIndex)
        where T : unmanaged, allows ref struct
    {
        parent._info.ThrowIfNull();
        var memberVector = parent._info.GetStructMemberVectorInfo(memberIndex, Unsafe.NullRef<DuckDbColumnInfo>());
        return new(memberVector);
    }

    /// <summary>
    /// Retrieve the vector for one member of a DuckDB struct.
    /// </summary>
    /// <param name="parentVector">
    /// A vector with elements typed as STRUCT in DuckDB.
    /// </param>
    /// <param name="memberIndex">
    /// The index of the member in the struct, numbered starting from 0.
    /// </param>
    /// <param name="columnInfo">
    /// Data type information for the member.  This argument may be a null reference, in which case
    /// this method will retrieve it,  The caller may supply it if it is already available, avoiding
    /// unnecessary API calls to the DuckDB native library.  
    /// </param>
    internal unsafe static DuckDbVectorInfo GetStructMemberVectorInfo(in this DuckDbVectorInfo parentVector, 
                                                                      int memberIndex,
                                                                      in DuckDbColumnInfo columnInfo)
    {
        CheckStructMemberIndex(parentVector, memberIndex);

        var memberVector = NativeMethods.duckdb_struct_vector_get_child(parentVector.NativeVector, memberIndex);
        if (memberVector == null)
            throw new DuckDbException("Could not get the member vector from a struct vector in DuckDB. ");

        return new DuckDbVectorInfo(
                    memberVector,
                    length: parentVector.Length,
                    columnInfo: Unsafe.IsNullRef(in columnInfo) ? new DuckDbColumnInfo(memberVector)
                                                                : columnInfo);
    }
}
