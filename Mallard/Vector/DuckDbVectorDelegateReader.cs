using System;

namespace Mallard;

/// <summary>
/// Provides access to a column of <see cref="DuckDbResultChunk" />.
/// </summary>
/// <remarks>
/// <para>
/// This type essentially provides the functionality of <see cref="DuckDbVectorReader{T}" />,
/// but without incorporating the vector element type as a generic parameter,
/// and without the restrictions of "ref structs".  It is necessary
/// to fit some API shapes, in particular ADO.NET.  Naturally, it has worse performance
/// characteristics (in order to maintain the same level of memory safety), 
/// so <see cref="DuckDbVectorReader{T}" /> remains the preferred
/// approach to reading DuckDB vectors.
/// </para>
/// <para>
/// The word "delegate" in the name of this type refers to its instances being like
/// delegates (in the .NET sense of the term) to the chunk's column/vector.
/// </para>
/// <para>
/// Because the .NET type for the vector elements does not get specified (at construction), instances
/// of this class assume the default type (mapping) as decided by this library.
/// </para>
/// </remarks>
public class DuckDbVectorDelegateReader : IDuckDbVector
{
    /// <summary>
    /// Holds on to reference to parent to prevent garbage collection while this instance
    /// still exists.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This object is the originating <see cref="DuckDbResult" />, but since it does not
    /// need to be directly accessed again, this field is typed as <see cref="object" />.
    /// </para>
    /// <para>
    /// Because <see cref="DuckDbVectorInfo" /> is passed by reference into 
    /// <see cref="VectorElementConverter.Convert{T}(in DuckDbVectorInfo, int, bool)" />,
    /// calls to <see cref="GC.KeepAlive(object?)" /> ought to be not needed during conversion.
    /// But there is a danger the .NET IL compiler would optimize out the references so
    /// we put in those calls anyway.
    /// </para>
    /// </remarks>
    private readonly object _owner;

    /// <summary>
    /// Points to one of the originating chunk's columns.
    /// </summary>
    private readonly DuckDbVectorInfo _vector;

    /// <summary>
    /// Converter for the defaulted .NET type for the DuckDB column.
    /// </summary>
    private readonly VectorElementConverter _converter;

    /// <summary>
    /// Converter for the boxed version of the defaulted .NET type for the DuckDB column.
    /// </summary>
    private readonly VectorElementConverter _boxedConverter;

    /// <summary>
    /// Copy of <see cref="IDuckDbVector.ValidityMask" /> of <see cref="_vector"/>, 
    /// allocated in managed memory.
    /// </summary>
    private ulong[]? _validityMaskCopy;

    /// <summary>
    /// Obtains read access to a column (vector) in a result chunk coming from DuckDB.
    /// </summary>
    /// <param name="chunk">
    /// The target chunk.  To enforce memory safety (in the presence of potentially
    /// multi-threaded access), the chunk can no longer be explicitly disposed
    /// once any reference to a column is taken using this class.  Subsequent calls
    /// to <see cref="DuckDbResultChunk.Dispose" /> will be silently ignored.
    /// </param>
    /// <param name="columnIndex">
    /// The index of the desired column.  Must be between 0 (inclusive)
    /// and <see cref="DuckDbResultChunk.ColumnCount" /> (exclusive).
    /// </param>
    public DuckDbVectorDelegateReader(DuckDbResultChunk chunk, int columnIndex)
    {
        chunk.IgnoreDisposals();
        _owner = chunk;
        _vector = chunk.UnsafeGetColumnVector(columnIndex);
        _converter = chunk.GetColumnConverter(columnIndex, null).BindToVector(_vector);
        _boxedConverter = chunk.GetColumnConverter(columnIndex, typeof(object)).BindToVector(_vector);
    }

    /// <summary>
    /// The variable-length bit mask indicating which elements in the vector are valid (not null).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike the efficient implementation from <see cref="DuckDbVectorReader{T}" />,
    /// this method must copy out the array of bits.  The original array is in memory natively
    /// allocated by DuckDB, and this class (not being a ref struct) 
    /// has no way to control the lifetime of the span were the original array to be returned.
    /// </para>
    /// <para>
    /// Clients that use <see "DuckDbVectorDelegateReader" /> instead of <see cref="DuckDbVectorReader{T}" />
    /// generally are not using performant span-based APIs anyway, so this fallback exists
    /// only to complete the implementation of the interface method (<see cref="IDuckDbVector.ValidityMask" />).
    /// </para>
    /// </remarks>
    ReadOnlySpan<ulong> IDuckDbVector.ValidityMask
    {
        get
        {
            var m = _validityMaskCopy;

            if (_validityMaskCopy == null)
            {
                var s = _vector.ValidityMask;
                if (s.Length == 0)
                {
                    m = Array.Empty<ulong>();
                }
                else
                {
                    m = new ulong[s.Length];
                    s.CopyTo(m);
                }

                GC.KeepAlive(this);
                _validityMaskCopy = m;
            }

            return new ReadOnlySpan<ulong>(m);
        }
    }

    /// <inheritdoc cref="IDuckDbVector.Length" />
    public int Length => _vector.Length;

    /// <inheritdoc cref="IDuckDbVector.ColumnInfo" />
    public DuckDbColumnInfo ColumnInfo => _vector.ColumnInfo;

    /// <inheritdoc cref="IDuckDbVector.IsItemValid(int)" />
    public bool IsItemValid(int index)
    {
        var b = _vector.IsItemValid(index);
        GC.KeepAlive(this);
        return b;
    }

    /// <summary>
    /// The .NET type that the elements in the DuckDB column are mapped to.
    /// </summary>
    public Type ElementType => _converter.TargetType;

    /// <summary>
    /// Get an item in the vector, cast into <see cref="System.Object"/>, or null if
    /// the selected item is invalid.
    /// </summary>
    /// <param name="index">The (row) index of the element of the vector. </param>
    public object? GetObjectOrNull(int index)
    {
        var v = _boxedConverter.Convert<object>(_vector, index, requireValid: false);
        GC.KeepAlive(this);
        return v;
    }

    /// <summary>
    /// Get an item in the vector, cast into <see cref="System.Object"/>.
    /// </summary>
    /// <param name="index">The (row) index of the element of the vector. </param>
    public object GetObject(int index)
    {
        var v = _boxedConverter.Convert<object>(_vector, index, requireValid: true)!;
        GC.KeepAlive(this);
        return v;
    }

    /// <summary>
    /// Throws the exception for the generic parameter to <see cref="GetValue" /> being wrong.
    /// </summary>
    private void ThrowExceptionForWrongType(Type receiverType)
    {
        throw new ArgumentException(
            "The generic type T that GetValue<T> has been called with is incompatible with the " +
            "actual type of the element from the DuckDB vector. " +
            $"Desired type: {receiverType}, Actual type: {_converter.TargetType}");
    }

    /// <summary>
    /// Get an item in the vector in its default .NET type (without boxing it).
    /// </summary>
    /// <typeparam name="T">
    /// <para>
    /// The .NET type of the column, required to be specified since it has been 
    /// "type-erased" from the type identity of this class.  However, it must 
    /// still match, at run-time, the actual type that the elements in the DuckDB
    /// column have been mapped to by default, as indicated by <see cref="ElementType" />.
    /// (For reference types, a base class or interface can also match.)
    /// </para>
    /// <para>
    /// This type should not be a nullable value type; it would never match
    /// <see cref="ElementType" />.  There is no "notnull" constraint 
    /// on <typeparamref name="T" /> only because some APIs that would be
    /// implemented with this class do not have that shape.
    /// </para>
    /// </typeparam>
    public T GetValue<T>(int index)
    {
        ref readonly VectorElementConverter converter = ref _boxedConverter;
        
        if (typeof(T) != typeof(object))
        {
            converter = ref _converter;
            if (!typeof(T).IsAssignableWithoutBoxingFrom(converter.TargetType))
                ThrowExceptionForWrongType(typeof(T));
        }

        var v = converter.Convert<T>(_vector, index, requireValid: true)!;
        GC.KeepAlive(this);
        return v;
    }
}
