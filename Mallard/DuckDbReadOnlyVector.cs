using Mallard.C_API;
using System;

namespace Mallard;

/// <summary>
/// Points to data for a column within a result chunk from DuckDB.
/// </summary>
/// <remarks>
/// DuckDB, a column-oriented database, calls this grouping of data a "vector".  
/// This type only supports reading from a DuckDB vector; writing to a vector
/// (for the purposes of modifying the database) requires a different shape of API
/// to enforce safety.
/// </remarks>
public unsafe readonly ref struct DuckDbReadOnlyVector<T> 
{
    /// <summary>
    /// "Vector" data structure obtained as part of a chunk from DuckDB.  It is
    /// de-allocated together with the chunk.
    /// </summary>
    internal readonly _duckdb_vector* _nativeVector;

    /// <summary>
    /// The basic type of data from DuckDB, used to verify correctly-typed access.
    /// </summary>
    private readonly DuckDbBasicType _basicType;

    /// <summary>
    /// The length (number of rows) inherited from the result chunk this vector is part of.
    /// </summary>
    internal readonly int _length;

    /// <summary>
    /// Pointer to the raw data array of the DuckDB vector. 
    /// </summary>
    internal readonly void* _nativeData;

    /// <summary>
    /// Pointer to the bit mask from DuckDB indicating whether the corresponding element
    /// in the array pointed to by <see cref="_nativeData"/> is valid (not null). 
    /// </summary>
    /// <remarks>
    /// This may be null if all elements in the array are valid.
    /// </remarks>
    internal readonly void* _validityMask;

    internal DuckDbReadOnlyVector(_duckdb_vector* nativeVector,
                                  DuckDbBasicType basicType,
                                  int length)
    {
        DuckDbReadOnlyVectorMethods.ThrowOnWrongClrType<T>(basicType);

        _nativeVector = nativeVector;
        _basicType = basicType;
        _length = length;

        _nativeData = NativeMethods.duckdb_vector_get_data(_nativeVector);
        _validityMask = NativeMethods.duckdb_vector_get_validity(_nativeVector);
    }

    public ReadOnlySpan<ulong> GetValidityMask()
    {
        return new ReadOnlySpan<ulong>(_validityMask, _validityMask != null ? _length : 0);
    }
}

public unsafe static partial class DuckDbReadOnlyVectorMethods 
{
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
    public static bool ValidateGenericType<T>(DuckDbBasicType basicType)
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
            _ => false,
        };
    }

    internal static void ThrowOnWrongClrType<T>(DuckDbBasicType basicType)
    {
        if (!ValidateGenericType<T>(basicType))
            throw new ArgumentException($"Generic type {typeof(T).Name} does not match the DuckDB basic type {basicType} of the elements in the desired column.");
    }

    internal static void ThrowOnNullVector(_duckdb_vector* vector)
    {
        if (vector == null)
            throw new InvalidOperationException("Cannot operate on a default instance of DuckDbReadOnlyVector. ");
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

    public static ReadOnlySpan<T> AsSpan<T>(in this DuckDbReadOnlyVector<T> vector) where T : unmanaged
    {
        return new ReadOnlySpan<T>(vector._nativeData, vector._length);
    }
}
