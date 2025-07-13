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
public unsafe readonly ref struct DuckDbReadOnlyVector
{
    /// <summary>
    /// "Vector" data obtained as part of a chunk from DuckDB.  It is
    /// de-allocated together with the chunk.
    /// </summary>
    private readonly _duckdb_vector* _nativeVector;

    /// <summary>
    /// The basic type of data from DuckDB, used to verify correctly-typed access.
    /// </summary>
    private readonly DuckDbBasicType _basicType;

    /// <summary>
    /// The length (number of rows) inherited from the result chunk this vector is part of.
    /// </summary>
    private readonly int _length;

    internal DuckDbReadOnlyVector(_duckdb_vector* nativeVector, 
                                  DuckDbBasicType basicType, 
                                  int length)
    {
        _nativeVector = nativeVector;
        _basicType = basicType;
        _length = length;
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
    private static bool ValidateGenericType<T>(DuckDbBasicType basicType) where T : unmanaged
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

    public ReadOnlySpan<T> AsSpan<T>() where T : unmanaged
    {
        // N.B. A default-initialized instance will always fail validation.
        if (!ValidateGenericType<T>(_basicType))
            throw new ArgumentException("Generic type T does not match type of data present in the result column. ");

        return new ReadOnlySpan<T>(NativeMethods.duckdb_vector_get_data(_nativeVector),
                                   _length);
    }

    public ReadOnlySpan<ulong> GetValidityMask()
    {
        // N.B. A default-initialized instance will get a null pointer p.
        var p = NativeMethods.duckdb_vector_get_validity(_nativeVector);
        return new ReadOnlySpan<ulong>(p, p != null ? _length : 0);
    }
}
