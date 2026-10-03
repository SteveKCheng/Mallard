using System;
using System.Diagnostics.CodeAnalysis;

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
/// Only "raw" fixed-width values can be written through this type.  Variable-length values
/// (<see cref="DuckDbValueKind.VarChar" /> / <see cref="DuckDbValueKind.Blob" />, represented by the
/// read-only <see cref="DuckDbString" /> and <see cref="DuckDbBlob" />) and nested types cannot be
/// written here, because DuckDB must allocate and manage their backing memory; attempting to set such
/// an element throws <see cref="NotSupportedException" />.  Use the row-at-a-time appender API for
/// those columns until converting ("non-raw") writers are available.  Individual elements may,
/// however, be marked SQL <c>NULL</c> with <see cref="SetInvalid" /> or through <see cref="ValidityMask" />.
/// </para>
/// <para>
/// Like the raw reader, this writer is a "ref struct" because it holds pointers to native memory
/// whose lifetime must be bounded.  Non-trivial instances are only accessible from within a
/// chunk-writing function conforming to <see cref="DuckDbChunkWritingFunc{TState}" />.
/// </para>
/// </remarks>
public unsafe readonly ref struct DuckDbVectorRawWriter<T>
    where T : unmanaged, allows ref struct
{
    /// <summary>
    /// Type information and native pointers on the DuckDB vector being written to.
    /// </summary>
    internal readonly DuckDbVectorInfo _info;

    /// <summary>
    /// Cached pointer to the validity mask for writing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// DuckDB does not allocate a validity mask until one is explicitly requested.
    /// That usually happens well after construction of this structure, but we
    /// want to cache the returned pointer from DuckDB so we do not have call
    /// P/Invoke to request it every time we set an item.  (Once allocated, the pointer
    /// is always the same, and is not invalidated until the containing chunk is reset or
    /// destroyed.) 
    /// </para>
    /// <para>
    /// However, this structure immutable so we cannot cache the pointer directly,
    /// but must store it inside some other object, whose location is passed in to constructor.
    /// (Even we made this structure mutable, the user could copy a structure in .NET at any time
    /// --- and the cached value could become outdated when stored directly as a member here.) 
    /// </para>
    /// <para>
    /// A value of null means there is no current validity mask (or it has the initial value
    /// of "all elements are valid").
    /// </para>
    /// <para>
    /// This cache relies on thread-exclusivity of the chunk writer, so no inter-thread
    /// synchronization is necessary. 
    /// </para>
    /// </remarks>
    private readonly ref ulong* _validityMask;

    internal DuckDbVectorRawWriter(scoped in DuckDbVectorInfo info, ref ulong* validityMask)
    {
        _info = info;
        _validityMask = ref validityMask;
        
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
    /// Obtain the vector whereby the validity of elements in the elements may be set directly
    /// (by bit manipulation).
    /// </summary>
    /// <remarks>
    /// <para>
    /// The indices of the validity mask is mapped in the same manner as described in
    /// <see cref="IDuckDbVector.ValidityMask" />.
    /// </para>
    /// <para>
    /// The validity mask will immediately reflect any changes made by <see cref="SetInvalid" />,
    /// or setting (non-null) values.
    /// </para>
    /// <para>
    /// Elements default to valid when the containing chunk is initialized, so
    /// accessing the validity mask need not be accessed if there are no NULL elements.
    /// </para>
    /// <para>
    /// When the source validity values are already batched, setting the bits directly through the returned span
    /// is faster than calling <see cref="SetInvalid" /> on each item.   
    /// </para>
    /// </remarks>
    public Span<ulong> ValidityMask => new(ValidityMaskPointer, DuckDbVectorInfo.GetValidityMaskLength(_info.Length));

    /// <summary>
    /// The pointer to the writable validity mask for this DuckDB vector; 
    /// cached version of <see cref="DuckDbVectorInfo.GetMutableValidityMask" />.
    /// </summary>
    private ulong* ValidityMaskPointer
        => (_validityMask == null) ? (_validityMask = _info.GetMutableValidityMask()) : _validityMask;

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
    /// <remarks>
    /// <para>
    /// If the element at <paramref name="index" /> was previously marked invalid via <see cref="SetInvalid" />,
    /// it becomes valid after a successful call to this method.
    /// </para>
    /// </remarks>
    /// <exception cref="NotSupportedException">
    /// <typeparamref name="T" /> is a variable-length type (<see cref="DuckDbString" /> or
    /// <see cref="DuckDbBlob" />), which cannot be written through a raw writer.
    /// </exception>
    public void SetItem(int index, T value)
    {
        if (typeof(T) == typeof(DuckDbArrayRef) || typeof(T) == typeof(DuckDbStructRef))
            DuckDbVectorMethods.ThrowForAccessingNonexistentItems(typeof(T));

        // DuckDbString / DuckDbBlob are read-only views over DuckDB-owned (variable-length) memory, so
        // the raw bitwise copy below would merely stamp a dangling string_t into the vector and corrupt
        // it.  Writing such values needs DuckDB to allocate their storage, which a raw writer does not do.
        if (typeof(T) == typeof(DuckDbString) || typeof(T) == typeof(DuckDbBlob))
            ThrowForUnwritableVariableLengthType(typeof(T));

        if (unchecked((uint)index >= (uint)_info.Length))
            throw new IndexOutOfRangeException("Index is out of range for the vector. ");

        _info.UnsafeWrite(index, value);

        // The item may have been marked invalid earlier; we must revert that
        UnsafeSetValid(index);
    }
    
    internal void UnsafeSetValid(int index) => DuckDbVectorInfo.UnsafeSetValid(_validityMask, index);

    [DoesNotReturn]
    private static void ThrowForUnwritableVariableLengthType(Type type)
    {
        throw new NotSupportedException(
            $"Values of type {type.Name} cannot be written through a raw vector writer, because DuckDB " +
            "must allocate and manage the memory for variable-length data such as VARCHAR or BLOB.  Use the " +
            "row-at-a-time appender API for such columns. ");
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
    /// Marking an element invalid causes DuckDB to allocate a validity mask for the whole vector if one
    /// does not already exist, so a column with no nulls incurs no such cost.
    /// </para>
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public void SetInvalid(int index)
    {
        if (unchecked((uint)index >= (uint)_info.Length))
            throw new IndexOutOfRangeException("Index is out of range for the vector. ");

        DuckDbVectorInfo.UnsafeSetInvalid(ValidityMaskPointer, index);
    }
}
