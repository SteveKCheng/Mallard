using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// Encapsulates type information and pointers to a DuckDB vector.
/// </summary>
/// <remarks>
/// The native memory behind the pointers can obviously go out of scope, 
/// so this structure is not made public.  This information is independent 
/// of the parameterized type in <see cref="DuckDbVectorReader{T}" />.
/// Also, it is stored in a plain structure, not a "ref struct", so 
/// other parts of this library can store them in arrays (as part of the
/// collection of all columns in a query result).
/// </remarks>
internal unsafe readonly struct DuckDbVectorInfo
{
    /// <summary>
    /// "Vector" data structure obtained as part of a chunk from DuckDB.  It is
    /// de-allocated together with the chunk.
    /// </summary>
    internal readonly _duckdb_vector* NativeVector;

    /// <summary>
    /// Pointer to the raw data array of the DuckDB vector. 
    /// </summary>
    internal readonly void* NativeData;

    /// <summary>
    /// Pointer to the bit mask from DuckDB indicating whether the corresponding element
    /// in the array pointed to by <see cref="NativeData"/> is valid (not null). 
    /// </summary>
    /// <remarks>
    /// This may be null if all elements in the array are valid.
    /// </remarks>
    private readonly ulong* _validityMask;

    /// <summary>
    /// The length (number of rows) inherited from the result chunk this vector is part of.
    /// </summary>
    internal readonly int Length;

    /// <summary>
    /// The basic type of data from DuckDB, used to verify correctly-typed access, 
    /// cast to <c>byte</c> to conserve space.
    /// </summary>
    private readonly byte _basicType;

    /// <summary>
    /// The actual DuckDB type used for storage, when the logical type is
    /// <see cref="DuckDbBasicType.Enum" /> or <see cref="DuckDbBasicType.Decimal" />.
    /// </summary>
    /// <remarks>
    /// Set to zero (<see cref="DuckDbBasicType.Invalid" /> if inapplicable. 
    /// </remarks>
    private readonly byte _storageType;

    /// <summary>
    /// The number of digits after the decimal point, when the logical type is
    /// <see cref="DuckDbBasicType.Decimal" />.
    /// </summary>
    /// <remarks>
    /// Set to zero if inapplicable. 
    /// </remarks>
    internal readonly byte DecimalScale;

    internal DuckDbVectorInfo(_duckdb_vector* nativeVector,
                              DuckDbBasicType basicType,
                              int length)
    {
        NativeVector = nativeVector;
        NativeData = NativeMethods.duckdb_vector_get_data(NativeVector);
        _validityMask = NativeMethods.duckdb_vector_get_validity(NativeVector);

        Length = length;
        _basicType = (byte)basicType;

        if (basicType == DuckDbBasicType.Decimal)
        {
            var (scale, storageType) = GetDecimalStorageInfo(NativeVector);
            DecimalScale = scale;
            _storageType = (byte)storageType;
        }
        else if (basicType == DuckDbBasicType.Enum)
        {
            var storageType = GetEnumStorageType(NativeVector);
            _storageType = (byte)storageType;
        }
        else
        {
            _storageType = _basicType;
        }
    }

    private static (byte Scale, DuckDbBasicType StorageType) GetDecimalStorageInfo(_duckdb_vector* vector)
    {
        var nativeType = NativeMethods.duckdb_vector_get_column_type(vector);
        if (nativeType == null)
            throw new DuckDbException("Could not query the logical type of a vector from DuckDB. ");

        try
        {
            return (Scale: NativeMethods.duckdb_decimal_scale(nativeType),
                    StorageType: NativeMethods.duckdb_decimal_internal_type(nativeType));
        }
        finally
        {
            NativeMethods.duckdb_destroy_logical_type(ref nativeType);
        }
    }

    private static DuckDbBasicType GetEnumStorageType(_duckdb_vector* vector)
    {
        var nativeType = NativeMethods.duckdb_vector_get_column_type(vector);
        if (nativeType == null)
            throw new DuckDbException("Could not query the logical type of a vector from DuckDB. ");

        try
        {
            return NativeMethods.duckdb_enum_internal_type(nativeType);
        }
        finally
        {
            NativeMethods.duckdb_destroy_logical_type(ref nativeType);
        }
    }

    public DuckDbBasicType BasicType => (DuckDbBasicType)_basicType;

    public DuckDbBasicType StorageType => (DuckDbBasicType)_storageType;

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
            ThrowIndexOutOfRange(index, Length);

        return _validityMask == null || (_validityMask[j >> 6] & (1u << (int)(j & 63))) != 0;
    }

    private static void ThrowIndexOutOfRange(int index, int length)
    {
        throw new IndexOutOfRangeException("Index is out of range for the vector. ");
    }

    internal void VerifyItemIsValid(int index)
    {
        if (!IsItemValid(index))
            ThrowForInvalidElement(index);
    }

    private static void ThrowForInvalidElement(int index)
    {
        throw new InvalidOperationException($"The element of the vector at index {index} is invalid (null). ");
    }
}
