using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// Describes a column resulting from a DuckDB query.
/// </summary>
/// <remarks>
/// <para>
/// In particular, the properties in this type give sufficient information to decode data 
/// coming in from a DuckDB column.
/// </para>
/// <para>
/// This .NET type is also used to describe a DuckDB column that is nested within another, 
/// e.g. a member of a structure (STRUCT in DuckDB SQL). 
/// </para>
/// </remarks>
public readonly struct DuckDbColumnInfo
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
    /// The name of the column.
    /// </summary>
    /// <remarks>
    /// If the column has no name, this property yields the empty string.
    /// </remarks>
    public string Name { get; }

    /// <summary>
    /// Initialize information on one column. 
    /// </summary>
    /// <param name="nativeResult">Query results where the column comes from. </param>
    /// <param name="columnIndex">Index of the selected column. </param>
    internal unsafe DuckDbColumnInfo(ref duckdb_result nativeResult, int columnIndex)
    {
        Name = NativeMethods.duckdb_column_name(ref nativeResult, columnIndex);
        
        DuckDbValueKind valueKind = NativeMethods.duckdb_column_type(ref nativeResult, columnIndex);
        _valueKind = (byte)valueKind;

        DuckDbValueKind storageKind = valueKind;

        if (valueKind == DuckDbValueKind.Decimal ||
            valueKind == DuckDbValueKind.Enum ||
            valueKind == DuckDbValueKind.Array ||
            valueKind == DuckDbValueKind.Struct)
        {
            using var holder = new NativeLogicalTypeHolder(
                NativeMethods.duckdb_column_logical_type(ref nativeResult, columnIndex));

            if (valueKind == DuckDbValueKind.Decimal)
            {
                DecimalScale = NativeMethods.duckdb_decimal_scale(holder.NativeHandle);
                storageKind = NativeMethods.duckdb_decimal_internal_type(holder.NativeHandle);
            }
            else if (valueKind == DuckDbValueKind.Enum)
            {
                storageKind = NativeMethods.duckdb_enum_internal_type(holder.NativeHandle);
                ElementSize = (int)NativeMethods.duckdb_enum_dictionary_size(holder.NativeHandle);
            }
            else if (valueKind == DuckDbValueKind.Array)
            {
                ElementSize = (int)NativeMethods.duckdb_array_type_array_size(holder.NativeHandle);
            }
            else if (valueKind == DuckDbValueKind.Struct)
            {
                ElementSize = (int)NativeMethods.duckdb_struct_type_child_count(holder.NativeHandle);
            }
        }

        _storageKind = (byte)storageKind;
    }
}
