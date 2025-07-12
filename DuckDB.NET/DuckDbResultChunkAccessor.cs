using DuckDB.C_API;
using System;

namespace DuckDB;

/// <summary>
/// Encapsulates user-defined code that accesses <see cref="DuckDbResultChunkAccessor" />.
/// </summary>
/// <typeparam name="TState">
/// Type of arbitrary state that the user-defined code can take.  The state may be represented by 
/// a "ref struct", although it is always passed in by value so that it is impossible
/// for the user-defined code to leak out dangling pointers to inside the current chunk
/// (unless unsafe code is used).
/// </typeparam>
/// <typeparam name="TResult">
/// Arbitrary return type from the user-defined function.  This type may not be a
/// "ref struct", to prevent dangling pointers to inside the current chunk from being
/// returned.
/// </typeparam>
/// <param name="chunk">
/// Gives (temporary) access to the current chunk.
/// </param>
/// <param name="state">
/// Arbitrary state that the user-defined code can take.
/// </param>
/// <returns>
/// Whatever is desired.  Typically the return value would be the 
/// result of some transformation in the chunk's data.
/// </returns>
public delegate TResult DuckDbResultChunkFunc<in TState, out TResult>(in DuckDbResultChunkAccessor chunk, TState state)
    where TState : allows ref struct;

/// <summary>
/// Gives access to the data within a result chunk.
/// </summary>
/// <remarks>
/// Access must be intermediated through this "ref struct" to ensure that pointers to memory
/// allocated by the DuckDB native library can only be read from within a restricted scope.
/// This "ref struct" is passed to the body of 
/// <see cref="DuckDbResultChunkFunc{TState}" /> and cannot be accessed outside of that
/// body.
/// </remarks>
public unsafe readonly ref struct DuckDbResultChunkAccessor
{
    private readonly _duckdb_data_chunk* _nativeChunk;
    private readonly DuckDbResult.ColumnInfo[] _columnInfo;
    private readonly int _length;

    internal DuckDbResultChunkAccessor(_duckdb_data_chunk* nativeChunk,
                                       DuckDbResult.ColumnInfo[] columnInfo,
                                       int length)
    {
        _nativeChunk = nativeChunk;
        _columnInfo = columnInfo;
        _length = length;
    }

    /// <summary>
    /// Get access to the data for one column for all the rows represented by this chunk.
    /// </summary>
    /// <param name="columnIndex">
    /// The index of the column.
    /// </param>
    /// <returns>
    /// <see cref="DuckDbReadOnlyVector" /> representing the data for the column.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="columnIndex"/> is out of range, or this instance is default-initialized.
    /// </exception>
    public DuckDbReadOnlyVector GetColumn(int columnIndex)
    {
        // In case the user calls this method on a default-initialized instance,
        // the native library will not crash on this call because it does
        // check _nativeChunk for null first, returning null in that case.
        var nativeVector = NativeMethods.duckdb_data_chunk_get_vector(_nativeChunk,
                                                                      columnIndex);
        if (nativeVector == null)
            throw new IndexOutOfRangeException("Column index is not in range. ");
        return new DuckDbReadOnlyVector(nativeVector, _columnInfo[columnIndex].BasicType, _length);
    }
}

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
    private readonly duckdb_type _basicType;

    /// <summary>
    /// The length (number of rows) inherited from the result chunk this vector is part of.
    /// </summary>
    private readonly int _length;

    internal DuckDbReadOnlyVector(_duckdb_vector* nativeVector, 
                                  duckdb_type basicType, 
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
    private static bool ValidateGenericType<T>(duckdb_type basicType)
    {
        return basicType switch
        {
            duckdb_type.DUCKDB_TYPE_BOOLEAN => typeof(T) == typeof(byte),
            duckdb_type.DUCKDB_TYPE_TINYINT => typeof(T) == typeof(sbyte),
            duckdb_type.DUCKDB_TYPE_SMALLINT => typeof(T) == typeof(short),
            duckdb_type.DUCKDB_TYPE_INTEGER => typeof(T) == typeof(int),
            duckdb_type.DUCKDB_TYPE_BIGINT => typeof(int) == typeof(long),
            duckdb_type.DUCKDB_TYPE_UTINYINT => typeof(T) == typeof(byte),
            duckdb_type.DUCKDB_TYPE_USMALLINT => typeof(T) == typeof(ushort),
            duckdb_type.DUCKDB_TYPE_UINTEGER => typeof(T) == typeof(uint),
            duckdb_type.DUCKDB_TYPE_UBIGINT => typeof(T) == typeof(ulong),
            duckdb_type.DUCKDB_TYPE_FLOAT => typeof(T) == typeof(float),
            duckdb_type.DUCKDB_TYPE_DOUBLE => typeof(T) == typeof(double),
            duckdb_type.DUCKDB_TYPE_DATE => typeof(T) == typeof(DuckDbDate),
            _ => false,
        };
    }

    public ReadOnlySpan<T> AsSpan<T>()
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
