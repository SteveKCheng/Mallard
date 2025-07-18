using System;

namespace Mallard;

/// <summary>
/// Points to data for a column within a result chunk from DuckDB.
/// </summary>
/// <typeparam name="T">The .NET type for the element type
/// of the vector, which must be layout-compatible with the storage type
/// used by DuckDB.  The precise list of types allowed are:
/// <list type="table">
///   <listheader>
///     <term>DuckDB type</term>
///     <description>Corresponding .NET types</description>
///   </listheader>
///   <item>
///     <term><see cref="DuckDbBasicType.Boolean"/></term>
///     <description><see cref="System.Byte"/></description>
///   </item>
///   <item>
///     <term>...</term>
///     <description>more...</description>
///   </item>
/// </list>
/// </typeparam>
/// <remarks>
/// <para>
/// DuckDB, a column-oriented database, calls this grouping of data a "vector".  
/// This type only supports reading from a DuckDB vector; writing to a vector
/// (for the purposes of modifying the database) requires a different shape of API
/// to enforce safety.
/// </para>
/// <para>
/// This "raw" version of the reader passes the data to the user directly from the native
/// memory loaded by the DuckDB library.  It does not perform any other translation
/// (to other .NET types).
/// </para>
/// </remarks>
public readonly ref struct DuckDbVectorRawReader<T> : IDuckDbVector
    where T : unmanaged, allows ref struct
{
    internal readonly DuckDbVectorInfo _info;

    internal DuckDbVectorRawReader(scoped in DuckDbVectorInfo info)
    {
        _info = info;
        if (!ValidateParamType(_info.StorageType))
            DuckDbVectorInfo.ThrowForWrongParamType(_info.BasicType, _info.StorageType, typeof(T));
    }

    /// <inheritdoc cref="IDuckDbVector.ValidityMask" />
    public ReadOnlySpan<ulong> ValidityMask => _info.ValidityMask;

    /// <inheritdoc cref="IDuckDbVector.Length" />
    public int Length => _info.Length;

    /// <inheritdoc cref="IDuckDbVector.IsItemValid(int)" />
    public bool IsItemValid(int index) => _info.IsItemValid(index);

    /// <summary>
    /// Validate that the .NET type is correct for interpreting the raw
    /// data array obtained from DuckDB.
    /// </summary>
    /// <typeparam name="T">The .NET type to check. </typeparam>
    /// <param name="basicType">The basic type of the DuckDB data array
    /// desired to be accessed. </param>
    /// <returns>
    /// True if the .NET type is correct; false if incorrect or
    /// the <paramref name="basicType" /> does not refer to data
    /// that can be directly interpreted from .NET.
    /// </returns>
    public static bool ValidateParamType(DuckDbBasicType basicType)
    {
        return basicType switch
        {
            DuckDbBasicType.Boolean => typeof(T) == typeof(byte),
            DuckDbBasicType.TinyInt => typeof(T) == typeof(sbyte),
            DuckDbBasicType.SmallInt => typeof(T) == typeof(short),
            DuckDbBasicType.Integer => typeof(T) == typeof(int),
            DuckDbBasicType.BigInt => typeof(int) == typeof(long),
            DuckDbBasicType.UTinyInt => typeof(T) == typeof(byte),
            DuckDbBasicType.USmallInt => typeof(T) == typeof(ushort),
            DuckDbBasicType.UInteger => typeof(T) == typeof(uint),
            DuckDbBasicType.UBigInt => typeof(T) == typeof(ulong),
            DuckDbBasicType.Float => typeof(T) == typeof(float),
            DuckDbBasicType.Double => typeof(T) == typeof(double),

            DuckDbBasicType.Date => typeof(T) == typeof(DuckDbDate),
            DuckDbBasicType.Timestamp => typeof(T) == typeof(DuckDbTimestamp),

            DuckDbBasicType.Interval => typeof(T) == typeof(DuckDbInterval),

            DuckDbBasicType.List => typeof(T) == typeof(DuckDbListRef),
            DuckDbBasicType.VarChar => typeof(T) == typeof(DuckDbString),
            DuckDbBasicType.UHugeInt => typeof(T) == typeof(UInt128),
            DuckDbBasicType.HugeInt => typeof(T) == typeof(Int128),
            DuckDbBasicType.Blob => typeof(T) == typeof(DuckDbString),
            DuckDbBasicType.Bit => typeof(T) == typeof(DuckDbString),
            DuckDbBasicType.Uuid => typeof(T) == typeof(UInt128),
            DuckDbBasicType.Decimal => typeof(T) == typeof(short) ||
                                       typeof(T) == typeof(int) ||
                                       typeof(T) == typeof(long) ||
                                       typeof(T) == typeof(Int128),
            DuckDbBasicType.Enum => typeof(T) == typeof(byte) ||
                                    typeof(T) == typeof(ushort) ||
                                    typeof(T) == typeof(uint),
            _ => false,
        };
    }

    /// <summary>
    /// Retrieve a valid element of this vector.
    /// </summary>
    /// <param name="index">The index of the element in this vector. </param>
    /// <returns>The desired element of this vector. </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    /// <exception cref="InvalidOperationException">The requested element is invalid. </exception>
    public T this[int index]
    {
        get
        {
            _info.VerifyItemIsValid(index);
            unsafe
            {
                return ((T*)_info.DataPointer)[index];
            }
        }
    }

    /// <summary>
    /// Retrieve an element of this vector, or report that it is invalid.
    /// </summary>
    /// <param name="index">The index of the element in this vector. </param>
    /// <param name="item">The item that is to be read out.  Set to the
    /// element type's default value when the element is invalid.
    /// </param>
    /// <returns>
    /// Whether the element is valid.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public unsafe bool TryGetItem(int index, out T item)
    {
        if (_info.IsItemValid(index))
        {
            item = ((T*)_info.DataPointer)[index];
            return true;
        }
        else
        {
            item = default;
            return false;
        }
    }
}

public unsafe static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the contents of a DuckDB vector as a .NET span.
    /// </summary>
    /// <typeparam name="T">
    /// The type of element in the DuckDB vector.  This method is only available for
    /// "primitive" types, i.e. types containing data of fixed size, as variable-length 
    /// data cannot be safely exposed except through a "ref struct", but a "ref struct"
    /// cannot be the element type of <see cref="ReadOnlySpan{T}" />.
    /// </typeparam>
    /// <param name="vector">The DuckDB vector to read from. </param>
    /// <returns>
    /// Span covering all the elements of the DuckDB vector.
    /// Note that elements of the vector that are invalid, may be "garbage" or un-initialized when
    /// indexed using the returned span. 
    /// </returns>
    public static ReadOnlySpan<T> AsSpan<T>(in this DuckDbVectorRawReader<T> vector) where T : unmanaged
        => new(vector._info.DataPointer, vector._info.Length);
}
