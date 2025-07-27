﻿using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// Describes the data type of (the items in) a column resulting from a DuckDB query.
/// </summary>
/// <remarks>
/// <para>
/// The properties in this type provide information to decode data 
/// coming in from a DuckDB column.  This information is sufficient for "simple" types
/// (such as integers, decimals).  
/// </para>
/// <para>
/// For "complex" types (of some composited/nested nature),
/// specialized API calls to the DuckDB native library may be necessary to decode/describe
/// them fully.  Those API calls are made available elsewhere in this library as they 
/// require more complex state/resource management.  This type optimizes for the by far the 
/// most common case, where basic information on the DuckDB type/column can be gathered 
/// cheaply upfront, and stored in an inert structure that can be passed around and accessed
/// (in .NET code) with no restrictions.
/// </para>
/// <para>
/// This .NET type is also used to describe a DuckDB column that is nested within another, 
/// e.g. a member of a structure (STRUCT in DuckDB SQL). 
/// </para>
/// <para>
/// The name of the column (if any) is not part of this information set, because 
/// it is irrelevant for decoding the data from the column.  By omitting the name,
/// this .NET type can also implement structural equality on the type information.
/// </para>
/// </remarks>
public readonly record struct DuckDbColumnInfo
{
    /// <summary>
    /// Backing field for <see cref="ValueKind" /> compressed to a byte.
    /// </summary>
    private readonly byte _valueKind;

    /// <summary>
    /// Backing field for <see cref="StorageKind" /> compressed to a byte.
    /// </summary>
    private readonly byte _storageKind;

    /// <summary>
    /// The number of digits after the decimal point, when the logical type is
    /// <see cref="DuckDbValueKind.Decimal" />.
    /// </summary>
    /// <remarks>
    /// Set to zero if inapplicable. 
    /// </remarks>
    public byte DecimalScale { get; }

    /// <summary>
    /// The kind of item data, used to verify correctly-typed access
    /// to items of the vector.
    /// </summary>
    public DuckDbValueKind ValueKind => (DuckDbValueKind)_valueKind;

    /// <summary>
    /// The actual representation kind used for storage within vectors, 
    /// when the logical type is
    /// <see cref="DuckDbValueKind.Enum" /> or <see cref="DuckDbValueKind.Decimal" />.
    /// </summary>
    /// <remarks>
    /// Set to zero (<see cref="DuckDbValueKind.Invalid" /> if inapplicable. 
    /// </remarks>
    public DuckDbValueKind StorageKind => (DuckDbValueKind)_storageKind;

    /// <summary>
    /// The "size" applicable to certain types of items that can be stored in a DuckDB vector.
    /// </summary>
    /// <remarks>
    /// <list type="table">
    ///   <listheader>
    ///     <term>Item type</term>
    ///     <description>What this property holds</description>
    ///   </listheader>
    ///   <item>
    ///     <term>Fixed-sized array (ARRAY in DuckDB SQL)</term>
    ///     <description>The size of those arrays</description>
    ///   </item>
    ///   <item>
    ///     <term>Enumeration (ENUM in DuckDB SQL)</term>
    ///     <description>The size (number of entries) in the enumeration type</description>
    ///   </item>
    ///   <item>
    ///     <term>Structures (STRUCT in DuckDB SQL)</term>    
    ///     <description>The number of members in the structural type</description>
    ///   </item>
    /// </list>
    /// </remarks>
    public int ElementSize { get; }

    /// <summary>
    /// Initialize information on one column of results from a DuckDB query.
    /// </summary>
    /// <param name="nativeResult">Query results where the column comes from. 
    /// This native object is borrowed for the duration of this constructor call.
    /// </param>
    /// <param name="columnIndex">Index of the selected column. </param>
    /// <remarks>
    /// <para>
    /// Functionally all the available constructors of <see cref="DuckDbColumnInfo" />
    /// result in the same value (same information), but owing to vagarities in DuckDB's C API,
    /// this version is more efficient, and should be used for the top-level columns of query
    /// results.
    /// </para>
    /// </remarks>
    internal unsafe DuckDbColumnInfo(ref duckdb_result nativeResult, int columnIndex)
    {
        DuckDbValueKind valueKind = NativeMethods.duckdb_column_type(ref nativeResult, columnIndex);
        DuckDbValueKind storageKind = valueKind;

        if (valueKind == DuckDbValueKind.Decimal ||
            valueKind == DuckDbValueKind.Enum ||
            valueKind == DuckDbValueKind.Array ||
            valueKind == DuckDbValueKind.Struct)
        {
            // Obtain native logical type object only if we need it
            using var holder = new NativeLogicalTypeHolder(
                NativeMethods.duckdb_column_logical_type(ref nativeResult, columnIndex));

            (storageKind, ElementSize, DecimalScale) = GatherSupplementaryInfo(valueKind, holder.NativeHandle);
        }

        _valueKind = (byte)valueKind;
        _storageKind = (byte)storageKind;
    }

    /// <summary>
    /// Initialize information from one vector (usually coming from a nested column). 
    /// </summary>
    /// <param name="nativeVector">The vector to retrieve type information from.
    /// This native object is borrowed for the duration of this constructor call. </param>
    internal unsafe DuckDbColumnInfo(_duckdb_vector* nativeVector)
    {
        using var holder = new NativeLogicalTypeHolder(
            NativeMethods.duckdb_vector_get_column_type(nativeVector));
        this = new DuckDbColumnInfo(holder.NativeHandle);
    }

    /// <summary>
    /// Initialize information from a native object for a logical type 
    /// (usually a child type from a column of composite type). 
    /// </summary>
    /// <param name="nativeLogicalType">
    /// Handle for the native DuckDB "logical type", borrowed for the duration of this constructor call.
    /// </param>
    internal unsafe DuckDbColumnInfo(_duckdb_logical_type* nativeLogicalType)
    {
        DuckDbValueKind valueKind = NativeMethods.duckdb_get_type_id(nativeLogicalType);

        DuckDbValueKind storageKind;
        (storageKind, ElementSize, DecimalScale) = GatherSupplementaryInfo(valueKind, nativeLogicalType);

        _valueKind = (byte)valueKind;
        _storageKind = (byte)storageKind;
    }

    /// <summary>
    /// Retrieve more detailed information from the DuckDB native library on how to
    /// decode/interpret a column's type of data.
    /// </summary>
    /// <param name="valueKind">
    /// The high-level kind of value for the column. 
    /// </param>
    /// <param name="nativeType">
    /// Native logical type object.  This method will borrow it to query the native library.
    /// </param>
    private unsafe static (DuckDbValueKind StorageKind, int ElementSize, byte DecimalScale)
        GatherSupplementaryInfo(DuckDbValueKind valueKind, _duckdb_logical_type* nativeType)
    {
        if (valueKind == DuckDbValueKind.Decimal)
        {
            return (StorageKind: NativeMethods.duckdb_decimal_internal_type(nativeType),
                    ElementSize: 0,
                    DecimalScale: NativeMethods.duckdb_decimal_scale(nativeType));
        }
        else if (valueKind == DuckDbValueKind.Enum)
        {
            return (StorageKind: NativeMethods.duckdb_enum_internal_type(nativeType),
                    ElementSize: (int)NativeMethods.duckdb_enum_dictionary_size(nativeType),
                    DecimalScale: 0);
        }
        else if (valueKind == DuckDbValueKind.Array)
        {
            return (StorageKind: valueKind,
                    ElementSize: (int)NativeMethods.duckdb_array_type_array_size(nativeType),
                    DecimalScale: 0);
        }
        else if (valueKind == DuckDbValueKind.Struct)
        {
            return (StorageKind: valueKind,
                    ElementSize: (int)NativeMethods.duckdb_struct_type_child_count(nativeType),
                    DecimalScale: 0);
        }
        else
        {
            return (StorageKind: valueKind,
                    ElementSize: 0,
                    DecimalScale: 0);
        }
    }
}
