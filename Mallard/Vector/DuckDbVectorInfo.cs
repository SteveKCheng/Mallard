using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Mallard;

/// <summary>
/// Description for a DuckDB vector containing basic type information, and 
/// pointers to the native data.
/// </summary>
/// <remarks>
/// <para>
/// The native memory behind the pointers can obviously go out of scope, 
/// so this structure is not made public.  This information is independent 
/// of the parameterized type in <see cref="DuckDbVectorReader{T}" />.
/// Also, it is stored in a plain structure, not a "ref struct", so 
/// other parts of this library can store them in arrays (as part of the
/// collection of all columns in a query result).
/// </para>
/// <para>
/// This structure is basically a combination of <see cref="DuckDbColumnInfo" />
/// (which describes the vector's type information)
/// and the pointers to the actual data.
/// </para>
/// </remarks>
internal unsafe readonly struct DuckDbVectorInfo
{
    /// <summary>
    /// Information on the column that this vector is part of.
    /// </summary>
    public DuckDbColumnInfo ColumnInfo { get; }

    /// <summary>
    /// "Vector" data structure obtained as part of a chunk from DuckDB.  It is
    /// de-allocated together with the chunk.
    /// </summary>
    internal _duckdb_vector* NativeVector { get; }

    /// <summary>
    /// Pointer to the raw data array of the DuckDB vector. 
    /// </summary>
    internal void* DataPointer { get; }

    /// <summary>
    /// Pointer to the bit mask from DuckDB indicating whether the corresponding element
    /// in the array pointed to by <see cref="DataPointer"/> is valid (not null). 
    /// </summary>
    /// <remarks>
    /// This may be null if all elements in the array are valid.
    /// </remarks>
    private readonly ulong* _validityMask;

    /// <summary>
    /// The length (number of rows) inherited from the result chunk this vector is part of.
    /// </summary>
    public int Length { get; }

    /// <summary>
    /// Construct descriptor on a given vector with cached column information.
    /// </summary>
    /// <param name="nativeVector">
    /// The vector containing the data (for one column within one chunk of the query results).
    /// </param>
    /// <param name="length">
    /// The length (number of rows) in the vector.  This information is generally
    /// cached at the level of the chunk containing all vectors (one for each column).
    /// </param>
    /// <param name="columnInfo">
    /// <para>
    /// Information on the column that the vector is part of.  All of this information
    /// (except for the name) can be obtained from <paramref name="nativeVector" />,
    /// but when processing multiple chunks from the same <see cref="DuckDbResult" />,
    /// the columns will always be the same so it is quicker to cache the information
    /// then to query the DuckDB native library every time.
    /// </para>
    /// <para>
    /// Pass the result of <see cref="DuckDbColumnInfo.DuckDbColumnInfo(_duckdb_vector*)" />
    /// if no cached column information is available.
    /// </para>
    /// </param>
    internal DuckDbVectorInfo(_duckdb_vector* nativeVector, int length, in DuckDbColumnInfo columnInfo)
    {
        ColumnInfo = columnInfo;

        NativeVector = nativeVector;

        // DuckDB's documentation says duckdb_vector_get_data should not be called for
        // the STRUCT type.  It does not say so for the ARRAY type, but since there is
        // a dedicated function to get the array data, we assume the same applies.
        if (columnInfo.ValueKind != DuckDbValueKind.Struct && columnInfo.ValueKind != DuckDbValueKind.Array)
            DataPointer = NativeMethods.duckdb_vector_get_data(nativeVector);

        _validityMask = NativeMethods.duckdb_vector_get_validity(nativeVector);

        Length = length;
    }

    internal static DuckDbVectorInfo FromNativeChunk(_duckdb_data_chunk* nativeChunk, 
                                                     IResultColumns resultColumns,
                                                     int length,
                                                     int columnIndex)
    {
        // In case the user calls this method on a default-initialized instance,
        // the native library will not crash on this call because it does
        // check _nativeChunk for null first, returning null in that case.
        var nativeVector = NativeMethods.duckdb_data_chunk_get_vector(nativeChunk,
                                                                      columnIndex);
        if (nativeVector == null)
            throw new IndexOutOfRangeException("Column index is not in range. ");

        return new DuckDbVectorInfo(nativeVector, length, resultColumns.GetColumnInfo(columnIndex));
    }

    /// <summary>
    /// Read an element of the vector from native memory.
    /// </summary>
    /// <typeparam name="T">
    /// .NET type that is layout-compatible with the type of element in the DuckDB vector.
    /// </typeparam>
    /// <param name="index">
    /// The index of the element.  Must be an index for a valid element of the vector.
    /// </param>
    /// <remarks>
    /// This method does no run-time checking whatsoever.  It is used to implement readers
    /// and converters internally in this library.  Nevertheless use this method when possible,
    /// instead of indexing <see cref="DataPointer" /> manually, so the places where we read
    /// from native memory can be easily audited.
    /// </remarks>
    /// <remarks>
    /// Read-only reference to the vector element.  
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ref readonly T UnsafeRead<T>(int index) where T : unmanaged, allows ref struct
        => ref ((T*)DataPointer)[index];

    /// <summary>
    /// Implementation of <see cref="DuckDbVectorReader{T}.ValidityMask" />.
    /// </summary>
    public ReadOnlySpan<ulong> ValidityMask
        => new(_validityMask, _validityMask != null ? Length : 0);

    /// <summary>
    /// Implementation of <see cref="DuckDbVectorReader{T}.IsItemValid" />.
    /// </summary>
    public bool IsItemValid(int index)
    {
        var j = unchecked((uint)index);
        if (unchecked(j >= (uint)Length))
            throw new IndexOutOfRangeException("Index is out of range for the vector. ");

        return _validityMask == null || (_validityMask[j >> 6] & (1u << (int)(j & 63))) != 0;
    }

    internal void VerifyItemIsValid(int index)
    {
        if (!IsItemValid(index))
            ThrowForInvalidElement(index);
    }

    [DoesNotReturn]
    internal static void ThrowForInvalidElement(int index)
    {
        throw new InvalidOperationException($"The element of the vector at index {index} is invalid (null). ");
    }

    [DoesNotReturn]
    internal static void ThrowForWrongParamType(in DuckDbColumnInfo columnInfo, Type paramType)
    {
        var valueKind = columnInfo.ValueKind;
        var storageKind = columnInfo.StorageKind;

        if (valueKind == storageKind)
        {
            throw new ArgumentException(
                $"Generic type {paramType.Name} does not match the DuckDB basic type {valueKind} of the elements in the desired column.");
        }
        else
        {
            throw new ArgumentException(
                $"Generic type {paramType.Name} does not match the DuckDB basic type {valueKind} [{storageKind}] of the elements in the desired column.");
        }
    }

    /// <summary>
    /// Validate that the .NET type is correct for interpreting the raw
    /// data array obtained from DuckDB.
    /// </summary>
    /// <typeparam name="T">The .NET type to check. </typeparam>
    /// <param name="valueKind">The basic type of the DuckDB data array
    /// desired to be accessed. </param>
    /// <returns>
    /// True if the .NET type is correct; false if incorrect or
    /// the <paramref name="valueKind" /> does not refer to data
    /// that can be directly interpreted from .NET.
    /// </returns>
    internal static bool ValidateElementType<T>(DuckDbValueKind valueKind) 
        where T : allows ref struct
    {
        return valueKind switch
        {
            DuckDbValueKind.Boolean => typeof(T) == typeof(byte) || typeof(T) == typeof(bool),

            DuckDbValueKind.TinyInt => typeof(T) == typeof(sbyte),
            DuckDbValueKind.SmallInt => typeof(T) == typeof(short),
            DuckDbValueKind.Integer => typeof(T) == typeof(int),
            DuckDbValueKind.BigInt => typeof(T) == typeof(long),
            DuckDbValueKind.UTinyInt => typeof(T) == typeof(byte),
            DuckDbValueKind.USmallInt => typeof(T) == typeof(ushort),
            DuckDbValueKind.UInteger => typeof(T) == typeof(uint),
            DuckDbValueKind.UBigInt => typeof(T) == typeof(ulong),
            DuckDbValueKind.Float => typeof(T) == typeof(float),
            DuckDbValueKind.Double => typeof(T) == typeof(double),

            DuckDbValueKind.Date => typeof(T) == typeof(DuckDbDate),
            DuckDbValueKind.Timestamp => typeof(T) == typeof(DuckDbTimestamp),

            DuckDbValueKind.Interval => typeof(T) == typeof(DuckDbInterval),

            DuckDbValueKind.List => typeof(T) == typeof(DuckDbListRef),
            DuckDbValueKind.Array => typeof(T) == typeof(DuckDbArrayRef),

            DuckDbValueKind.VarChar => typeof(T) == typeof(DuckDbString) || typeof(T) == typeof(DuckDbBlob),
            DuckDbValueKind.VarInt => typeof(T) == typeof(DuckDbVarInt),
            DuckDbValueKind.Bit => typeof(T) == typeof(DuckDbBitString),

            DuckDbValueKind.UHugeInt => typeof(T) == typeof(UInt128),
            DuckDbValueKind.HugeInt => typeof(T) == typeof(Int128),
            DuckDbValueKind.Blob => typeof(T) == typeof(DuckDbBlob),
            DuckDbValueKind.Uuid => typeof(T) == typeof(DuckDbUuid) || typeof(T) == typeof(UInt128),
            DuckDbValueKind.Decimal => typeof(T) == typeof(short) ||
                                       typeof(T) == typeof(int) ||
                                       typeof(T) == typeof(long) ||
                                       typeof(T) == typeof(Int128),
            DuckDbValueKind.Enum => typeof(T) == typeof(byte) ||
                                    typeof(T) == typeof(ushort) ||
                                    typeof(T) == typeof(uint),

            DuckDbValueKind.Struct => typeof(T) == typeof(DuckDbStructRef),
            _ => false,
        };
    }

    internal static DuckDbValueKind GetVectorElementValueKind(_duckdb_vector* vector)
    {
        var nativeType = NativeMethods.duckdb_vector_get_column_type(vector);
        if (nativeType == null)
            throw new DuckDbException("Could not query the logical type of a vector from DuckDB. ");

        try
        {
            return NativeMethods.duckdb_get_type_id(nativeType);
        }
        finally
        {
            NativeMethods.duckdb_destroy_logical_type(ref nativeType);
        }
    }

    internal void ThrowIfNull()
    {
        if (NativeVector == null)
            throw new InvalidOperationException("Cannot operate on a default instance of DuckDbReadOnlyVector. ");
    }
}
