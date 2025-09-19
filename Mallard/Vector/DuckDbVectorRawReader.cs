using System;

namespace Mallard;

/// <summary>
/// Points to data for a column within a result chunk from DuckDB,
/// and gives read access to it.
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
///     <term><see cref="DuckDbValueKind.Boolean"/></term>
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
/// higher-level like <see cref="DuckDbValueKind.Decimal" /> or nested ones like
/// <see cref="DuckDbValueKind.List" />.  Results that are easier to consume can
/// be produced by the non-raw <see cref="DuckDbVectorReader{T}" /> instead,
/// at the expense of some efficiency.
/// </para>
/// <para>
/// In theory, any type can be retrieved and converted in the most efficient manner 
/// by generating source code that reads the raw data through this reader.  But
/// source generation may be overkill and complicated for many applications.
/// </para>
/// <para>
/// The reader is a "ref struct" because internally it holds and accesses pointers
/// to native memory for the vector from DuckDB, and so its scope (lifetime) must be
/// carefully controlled.  Non-trivial instances are only accessible from within a processing
/// function for a chunk conforiming to <see cref="DuckDbChunkReadingFunc{TState, TReturn}" />.
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
        if (!ValidateParamType(_info.ColumnInfo.StorageKind))
            DuckDbVectorInfo.ThrowForWrongParamType(_info.ColumnInfo, typeof(T));
    }

    /// <inheritdoc cref="IDuckDbVector.ValidityMask" />
    public ReadOnlySpan<ulong> ValidityMask => _info.ValidityMask;

    /// <inheritdoc cref="IDuckDbVector.Length" />
    public int Length => _info.Length;

    /// <inheritdoc cref="IDuckDbVector.IsItemValid(int)" />
    public bool IsItemValid(int index) => _info.IsItemValid(index);

    /// <inheritdoc cref="IDuckDbVector.ColumnInfo" />
    public DuckDbColumnInfo ColumnInfo => _info.ColumnInfo;

    /// <summary>
    /// Validate that the .NET type is correct for interpreting the raw
    /// data array obtained from DuckDB.
    /// </summary>
    /// <param name="valueKind">The basic type of the DuckDB data array
    /// desired to be accessed. </param>
    /// <returns>
    /// True if the .NET type is correct; false if incorrect or
    /// the <paramref name="valueKind" /> does not refer to data
    /// that can be directly interpreted from .NET.
    /// </returns>
    public static bool ValidateParamType(DuckDbValueKind valueKind)
        => DuckDbVectorInfo.ValidateElementType<T>(valueKind);

    /// <summary>
    /// Same as <see cref="GetItem" />: retrieve a valid element of this vector.
    /// </summary>
    /// <param name="index">The index of the element in this vector. </param>
    /// <returns>The desired element of this vector. </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    /// <exception cref="InvalidOperationException">The requested element is invalid. </exception>
    public T this[int index] => GetItem(index);

    /// <inheritdoc cref="IDuckDbVector{T}.GetItemOrDefault(int)" />
    public T GetItemOrDefault(int index)
    {
        TryGetItem(index, out var result);
        return result;
    }

    /// <inheritdoc cref="IDuckDbVector{T}.GetItem(int)" />
    public unsafe T GetItem(int index)
    {
        if (typeof(T) == typeof(DuckDbArrayRef) || typeof(T) == typeof(DuckDbStructRef))
            DuckDbVectorMethods.ThrowForAccessingNonexistentItems(typeof(T));

        _info.VerifyItemIsValid(index);
        return _info.UnsafeRead<T>(index);
    }

    /// <inheritdoc cref="IDuckDbVector{T}.TryGetItem(int, out T)" />
    public unsafe bool TryGetItem(int index, out T item)
    {
        if (typeof(T) == typeof(DuckDbArrayRef) || typeof(T) == typeof(DuckDbStructRef))
            DuckDbVectorMethods.ThrowForAccessingNonexistentItems(typeof(T));

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
