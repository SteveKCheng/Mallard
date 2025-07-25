using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// Dummy element type for <see cref="DuckDbVectorRawReader{T}" /> to consume
/// fixed-length arrays from a DuckDB vector.
/// </summary>
public readonly struct DuckDbArrayRef
{
    internal unsafe static long GetArraySize(in DuckDbVectorInfo parent)
    {
        var nativeType = NativeMethods.duckdb_vector_get_column_type(parent.NativeVector);
        if (nativeType == null)
            throw new DuckDbException("Could not query the logical type of a vector from DuckDB. ");

        try
        {
            return NativeMethods.duckdb_array_type_array_size(nativeType);
        }
        finally
        {
            NativeMethods.duckdb_destroy_logical_type(ref nativeType);
        }
    }
}

public static partial class DuckDbVectorMethods
{
    public static DuckDbVectorRawReader<T> GetChildrenRawVector<T>(in this DuckDbVectorRawReader<DuckDbArrayRef> parent)
        where T : unmanaged, allows ref struct
        => new(parent._info.GetArrayChildrenVectorInfo());

    internal unsafe static DuckDbVectorInfo GetArrayChildrenVectorInfo(in this DuckDbVectorInfo parent)
    {
        var parentVector = parent.NativeVector;
        DuckDbVectorInfo.ThrowOnNullVector(parentVector);

        var childVector = NativeMethods.duckdb_array_vector_get_child(parentVector);
        if (childVector == null)
            throw new DuckDbException("Could not get the child vector from an array vector in DuckDB. ");

        // FIXME This creates the logical type twice.  Re-factor code so logical type is only created once.
        // FIXME The array length needs to be exposed as a public property or method in
        //       DuckDbVectorRawReader<DuckDbArrayRef>.  Need some code re-organization to do so.
        var totalChildren = checked((int)(parent.Length * DuckDbArrayRef.GetArraySize(parent)));
        var childValueKind = DuckDbVectorInfo.GetVectorElementValueKind(childVector);

        return new DuckDbVectorInfo(childVector, childValueKind, totalChildren);
    }
}
