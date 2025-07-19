using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;

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
    internal readonly void* DataPointer;

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
        DataPointer = NativeMethods.duckdb_vector_get_data(NativeVector);
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

    [DoesNotReturn]
    private static void ThrowIndexOutOfRange(int index, int length)
    {
        throw new IndexOutOfRangeException("Index is out of range for the vector. ");
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
    internal static void ThrowForWrongParamType(DuckDbBasicType basicType, 
                                                DuckDbBasicType storageType,
                                                Type paramType)
    {
        if (basicType == storageType)
        {
            throw new ArgumentException(
                $"Generic type {paramType.Name} does not match the DuckDB basic type {basicType} of the elements in the desired column.");
        }
        else
        {
            throw new ArgumentException(
                $"Generic type {paramType.Name} does not match the DuckDB basic type {basicType} [{storageType}] of the elements in the desired column.");
        }
    }

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
    internal static bool ValidateElementType<T>(DuckDbBasicType basicType) 
        where T : allows ref struct
    {
        return basicType switch
        {
            DuckDbBasicType.Boolean => typeof(T) == typeof(byte) || typeof(T) == typeof(bool),

            DuckDbBasicType.TinyInt => typeof(T) == typeof(sbyte),
            DuckDbBasicType.SmallInt => typeof(T) == typeof(short),
            DuckDbBasicType.Integer => typeof(T) == typeof(int),
            DuckDbBasicType.BigInt => typeof(T) == typeof(long),
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

    internal static DuckDbBasicType GetVectorElementBasicType(_duckdb_vector* vector)
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

    internal static void ThrowOnNullVector(_duckdb_vector* vector)
    {
        if (vector == null)
            throw new InvalidOperationException("Cannot operate on a default instance of DuckDbReadOnlyVector. ");
    }
}
