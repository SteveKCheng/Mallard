using System;

namespace Mallard;
using Mallard.Types;

/// <summary>
/// Points to the data for a column within a data chunk that is being constructed for
/// appending to DuckDB, and gives write access to it.
/// </summary>
/// <typeparam name="T">The .NET type for the element type of the vector, which must be
/// layout-compatible with the storage type used by DuckDB.  The accepted types are the same
/// as for <see cref="DuckDbVectorRawReader{T}" />.
/// </typeparam>
/// <remarks>
/// <para>
/// This type is the write-side mirror of <see cref="DuckDbVectorRawReader{T}" />.  It is
/// deliberately kept separate from the reader: an instance only ever writes to a vector, and
/// provides no methods to read back what has been written.  This gives compile-time assurance
/// that a reader cannot be used to mutate the database and that a writer cannot be used to read.
/// </para>
/// <para>
/// This "raw" writer stores values directly into the native memory that DuckDB allocated for the
/// vector, performing no type conversion.  It is therefore only available for "primitive" types
/// of fixed size whose .NET representation is identical to DuckDB's storage representation.
/// </para>
/// <para>
/// Writing variable-length data (such as <see cref="DuckDbValueKind.VarChar" /> or nested types),
/// and marking elements as invalid (null), are not supported by this type in its current form.
/// </para>
/// <para>
/// Like the raw reader, this writer is a "ref struct" because it holds pointers to native memory
/// whose lifetime must be bounded.  Non-trivial instances are only accessible from within a
/// chunk-writing function conforming to <see cref="DuckDbChunkWritingFunc{TState}" />.
/// </para>
/// </remarks>
public readonly ref struct DuckDbVectorRawWriter<T>
    where T : unmanaged, allows ref struct
{
    /// <summary>
    /// Type information and native pointers on the DuckDB vector being written to.
    /// </summary>
    internal readonly DuckDbVectorInfo _info;

    internal DuckDbVectorRawWriter(scoped in DuckDbVectorInfo info)
    {
        _info = info;
        if (!ValidateParamType(_info.ColumnInfo.StorageKind))
            DuckDbVectorInfo.ThrowForWrongParamType(_info.ColumnInfo, typeof(T));
    }

    /// <summary>
    /// The number of elements that this vector can hold, i.e. the capacity of the data chunk.
    /// </summary>
    /// <remarks>
    /// Indices passed to <see cref="SetItem" /> (or the indexer) must be less than this value.
    /// This is the chunk's capacity (<c>duckdb_vector_size()</c>, normally 2048), not the number
    /// of rows that will ultimately be appended, which is reported separately by the return value
    /// of the chunk-writing function.
    /// </remarks>
    public int Capacity => _info.Length;

    /// <summary>
    /// Information about the column that this vector is part of.
    /// </summary>
    public DuckDbColumnInfo ColumnInfo => _info.ColumnInfo;

    /// <summary>
    /// Validate that the .NET type is correct for writing into the raw data array of the
    /// selected DuckDB column.
    /// </summary>
    /// <param name="valueKind">The basic type of the DuckDB data array to be written. </param>
    /// <returns>
    /// True if the .NET type is correct; false if incorrect or the <paramref name="valueKind" />
    /// does not refer to data that can be directly written from .NET.
    /// </returns>
    public static bool ValidateParamType(DuckDbValueKind valueKind)
        => DuckDbVectorInfo.ValidateElementType<T>(valueKind);

    /// <summary>
    /// Store one element into this vector.
    /// </summary>
    /// <param name="index">The index of the element in this vector. </param>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public T this[int index]
    {
        set => SetItem(index, value);
    }

    /// <summary>
    /// Store one element into this vector.
    /// </summary>
    /// <param name="index">
    /// The index of the element in this vector; must be non-negative and less than
    /// <see cref="Capacity" />.
    /// </param>
    /// <param name="value">The value to store. </param>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public void SetItem(int index, T value)
    {
        if (typeof(T) == typeof(DuckDbArrayRef) || typeof(T) == typeof(DuckDbStructRef))
            DuckDbVectorMethods.ThrowForAccessingNonexistentItems(typeof(T));

        if (unchecked((uint)index >= (uint)_info.Length))
            throw new IndexOutOfRangeException("Index is out of range for the vector. ");

        _info.UnsafeWrite(index, value);
    }

    /// <summary>
    /// Mark an element of this vector as invalid, i.e. SQL <c>NULL</c>.
    /// </summary>
    /// <param name="index">
    /// The index of the element in this vector; must be non-negative and less than
    /// <see cref="Capacity" />.
    /// </param>
    /// <remarks>
    /// <para>
    /// Every element is valid by default, so only elements that should be <c>NULL</c> need this call.
    /// Whatever data was (or was not) written at <paramref name="index" /> with <see cref="SetItem" />
    /// or the span is ignored by DuckDB once the element is marked invalid.
    /// </para>
    /// <para>
    /// Marking an element invalid causes DuckDB to allocate a validity mask for the whole vector if one
    /// does not already exist, so a column with no nulls incurs no such cost.
    /// </para>
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public void SetInvalid(int index)
    {
        if (unchecked((uint)index >= (uint)_info.Length))
            throw new IndexOutOfRangeException("Index is out of range for the vector. ");

        _info.UnsafeSetInvalid(index);
    }
}
