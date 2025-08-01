﻿using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// Dummy element type for <see cref="DuckDbVectorRawReader{T}" /> to consume
/// fixed-length arrays from a DuckDB vector.
/// </summary>
public readonly struct DuckDbArrayRef
{
}

public static partial class DuckDbVectorMethods
{
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
