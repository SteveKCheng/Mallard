using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

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
    /// <see cref="DuckDbValueKind.Enum" /> or <see cref="DuckDbValueKind.Decimal" />.
    /// </summary>
    /// <remarks>
    /// Set to zero (<see cref="DuckDbValueKind.Invalid" /> if inapplicable. 
    /// </remarks>
    private readonly byte _storageType;

    /// <summary>
    /// The number of digits after the decimal point, when the logical type is
    /// <see cref="DuckDbValueKind.Decimal" />.
    /// </summary>
    /// <remarks>
    /// Set to zero if inapplicable. 
    /// </remarks>
    internal readonly byte DecimalScale;

    internal DuckDbVectorInfo(_duckdb_vector* nativeVector,
                              DuckDbValueKind valueKind,
                              int length)
    {
        NativeVector = nativeVector;
        DataPointer = NativeMethods.duckdb_vector_get_data(NativeVector);
        _validityMask = NativeMethods.duckdb_vector_get_validity(NativeVector);

        Length = length;
        _basicType = (byte)valueKind;

        if (valueKind == DuckDbValueKind.Decimal)
        {
            var (scale, storageType) = GetDecimalStorageInfo(NativeVector);
            DecimalScale = scale;
            _storageType = (byte)storageType;
        }
        else if (valueKind == DuckDbValueKind.Enum)
        {
            var storageType = GetEnumStorageType(NativeVector);
            _storageType = (byte)storageType;
        }
        else
        {
            _storageType = _basicType;
        }
    }

    private static (byte Scale, DuckDbValueKind StorageType) GetDecimalStorageInfo(_duckdb_vector* vector)
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

    private static DuckDbValueKind GetEnumStorageType(_duckdb_vector* vector)
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

    public DuckDbValueKind ValueKind => (DuckDbValueKind)_basicType;

    public DuckDbValueKind StorageType => (DuckDbValueKind)_storageType;

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
    internal static void ThrowForWrongParamType(DuckDbValueKind valueKind, 
                                                DuckDbValueKind storageType,
                                                Type paramType)
    {
        if (valueKind == storageType)
        {
            throw new ArgumentException(
                $"Generic type {paramType.Name} does not match the DuckDB basic type {valueKind} of the elements in the desired column.");
        }
        else
        {
            throw new ArgumentException(
                $"Generic type {paramType.Name} does not match the DuckDB basic type {valueKind} [{storageType}] of the elements in the desired column.");
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

            DuckDbValueKind.VarChar => typeof(T) == typeof(DuckDbString),
            DuckDbValueKind.VarInt => typeof(T) == typeof(DuckDbVarInt),
            DuckDbValueKind.Bit => typeof(T) == typeof(DuckDbBitString),

            DuckDbValueKind.UHugeInt => typeof(T) == typeof(UInt128),
            DuckDbValueKind.HugeInt => typeof(T) == typeof(Int128),
            DuckDbValueKind.Blob => typeof(T) == typeof(DuckDbBlob),
            DuckDbValueKind.Uuid => typeof(T) == typeof(UInt128),
            DuckDbValueKind.Decimal => typeof(T) == typeof(short) ||
                                       typeof(T) == typeof(int) ||
                                       typeof(T) == typeof(long) ||
                                       typeof(T) == typeof(Int128),
            DuckDbValueKind.Enum => typeof(T) == typeof(byte) ||
                                    typeof(T) == typeof(ushort) ||
                                    typeof(T) == typeof(uint),
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

    internal static void ThrowOnNullVector(_duckdb_vector* vector)
    {
        if (vector == null)
            throw new InvalidOperationException("Cannot operate on a default instance of DuckDbReadOnlyVector. ");
    }
}
