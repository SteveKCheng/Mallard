using System;
using System.Diagnostics;
using Mallard.Interop;

namespace Mallard;

/// <summary>
/// Base class for descriptions of a complex type from DuckDB.
/// </summary>
public abstract class DuckDbComplexTypeInfo
{
    private static unsafe NativeLogicalTypeHolder MapToNativeLogicalTypePrimitive(DuckDbValueKind kind)
    {
        Debug.Assert(kind != DuckDbValueKind.Decimal &&
                     kind != DuckDbValueKind.Enum &&
                     kind != DuckDbValueKind.List &&
                     kind != DuckDbValueKind.Struct &&
                     kind != DuckDbValueKind.Map &&
                     kind != DuckDbValueKind.Array &&
                     kind != DuckDbValueKind.Union &&
                     kind != DuckDbValueKind.Invalid &&
                     kind is >= 0 and <= DuckDbValueKind.Variant);

        return new NativeLogicalTypeHolder(NativeMethods.duckdb_create_logical_type(kind));
    }
    
    /// <summary>
    /// Map a .NET type to the corresponding "logical type" from the DuckDB C API.
    /// </summary>
    /// <param name="type">
    /// The .NET type representing the desired column type.
    /// </param>
    /// <returns>
    /// The "logical type" object from the DuckDB C API that represents <paramref name="type" />.
    /// The caller owns the resulting object.
    /// </returns>
    /// <remarks>
    /// This function is typically used when creating data chunks in DuckDB for writing,
    /// and the DuckDB API requires that the client supply the type to be stored in each column.
    /// </remarks>
    internal static unsafe NativeLogicalTypeHolder MapToNativeLogicalType(Type type)
    {
        //
        // Primitive types
        //
        
        if (type == typeof(bool)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.Boolean);
        
        if (type == typeof(sbyte)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.TinyInt);
        if (type == typeof(short)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.SmallInt);
        if (type == typeof(int)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.Integer);
        if (type == typeof(long)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.BigInt);

        if (type == typeof(byte)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.UTinyInt);
        if (type == typeof(ushort)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.USmallInt);
        if (type == typeof(uint)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.UInteger);
        if (type == typeof(ulong)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.UBigInt);
       
        if (type == typeof(float)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.Float);
        if (type == typeof(double)) return MapToNativeLogicalTypePrimitive(DuckDbValueKind.Double);

        throw new NotSupportedException("Given .NET type cannot be mapped to a native DuckDB logical type. ");
    }
}
