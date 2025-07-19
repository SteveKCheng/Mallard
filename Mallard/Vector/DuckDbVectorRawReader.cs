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
/// <para>
/// Elements can be accessed one by one through 
/// Call <see cref="DuckDbVectorMethods.AsSpan" /> to obtain the 
/// </para>
/// <para>
/// This "raw" data may be difficult to consume, particularly for elements that are 
/// higher-level like <see cref="DuckDbBasicType.Decimal" /> or nested ones like
/// <see cref="DuckDbBasicType.List" />.  Results that are easier to consume can
/// be produced by the non-raw <see cref="DuckDbVectorReader{T}" /> instead
/// at the expense of some efficiency.
/// </para>
/// <para>
/// In theory, any type can be retrieved and converted in the most efficient manner 
/// by generating source code that reads the raw data through this reader.  But
/// source generation may be overkill and complicated for many applications.
/// </para>
/// </remarks>
public readonly ref struct DuckDbVectorRawReader<T> : IDuckDbVector<T>
    where T : unmanaged, allows ref struct
{
    /// <summary>
    /// Type information and native pointers on this DuckDB vector.
    /// </summary>
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
    /// The number of digits after the decimal point, when the logical type is
    /// <see cref="DuckDbBasicType.Decimal" />.
    /// </summary>
    /// <remarks>
    /// Set to zero if inapplicable. 
    /// </remarks>
    public byte DecimalScale => _info.DecimalScale;

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
        => DuckDbVectorInfo.ValidateElementType<T>(basicType);

    /// <summary>
    /// Same as <see cref="GetItem" />: retrieve a valid element of this vector.
    /// </summary>
    /// <param name="index">The index of the element in this vector. </param>
    /// <returns>The desired element of this vector. </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    /// <exception cref="InvalidOperationException">The requested element is invalid. </exception>
    public T this[int index] => GetItem(index);

    /// <inheritdoc cref="IDuckDbVector{T}.GetItem(int)" />
    public unsafe T GetItem(int index)
    {
        _info.VerifyItemIsValid(index);
        return _info.UnsafeRead<T>(index);
    }

    /// <inheritdoc cref="IDuckDbVector{T}.TryGetItem(int, out T)" />
    public unsafe bool TryGetItem(int index, out T item)
    {
        if (_info.IsItemValid(index))
        {
            item = _info.UnsafeRead<T>(index);
            return true;
        }
        else
        {
            item = default;
            return false;
        }
    }
}

public static partial class DuckDbVectorMethods
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
    public unsafe static ReadOnlySpan<T> AsSpan<T>(in this DuckDbVectorRawReader<T> vector) where T : unmanaged
        => new(vector._info.DataPointer, vector._info.Length);
}
