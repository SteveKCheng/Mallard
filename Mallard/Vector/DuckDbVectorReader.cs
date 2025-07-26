using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// Points to data for a column within a result chunk from DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// DuckDB, a column-oriented database, calls this grouping of data a "vector".  
/// This type only supports reading from a DuckDB vector; writing to a vector
/// (for the purposes of modifying the database) requires a different shape of API
/// to enforce safety.
/// </para>
/// <para>
/// This reader internally uses indirect calls to read, and convert if necessary,
/// the data in DuckDB's native formats, to the desired .NET type.  It is thus
/// slower than <see cref="DuckDbVectorRawReader{T}" /> but the results are easier
/// for clients to consume.
/// </para>
/// </remarks>
public unsafe readonly ref struct 
    DuckDbVectorReader<T> : IDuckDbVector<T>
{
    /// <summary>
    /// Type information and native pointers on this DuckDB vector.
    /// </summary>
    internal readonly DuckDbVectorInfo _info;

    /// <summary>
    /// Makes an indirect call to converts a DuckDB vector element to 
    /// an instance of <typeparamref name="T" />.
    /// </summary>
    private readonly VectorElementConverter _converter;

    internal bool DefaultValueIsInvalid => _converter.DefaultValueIsInvalid;

    internal DuckDbVectorReader(scoped in DuckDbVectorInfo info)
    {
        _info = info;

        var context = new ConverterCreationContext(info.ColumnInfo, info.NativeVector);
        _converter = VectorElementConverter.CreateForType(typeof(T), in context);

        if (!_converter.IsValid)
            DuckDbVectorInfo.ThrowForWrongParamType(info.ValueKind, info.StorageType, typeof(T));
    }

    /// <inheritdoc cref="IDuckDbVector.ValidityMask" />
    public ReadOnlySpan<ulong> ValidityMask => _info.ValidityMask;

    /// <inheritdoc cref="IDuckDbVector.IsItemValid(int)" />
    public bool IsItemValid(int index) => _info.IsItemValid(index);

    /// <inheritdoc cref="IDuckDbVector.Length" />
    public int Length => _info.Length;

    /// <inheritdoc cref="IDuckDbVector.ColumnInfo" />
    public DuckDbColumnInfo ColumnInfo => _info.ColumnInfo;

    /// <inheritdoc cref="IDuckDbVector{T}.GetItemOrDefault(int)" />
    public T? GetItemOrDefault(int index)
        => _converter.Convert<T>(_info, index, requireValid: false);

    /// <inheritdoc cref="IDuckDbVector{T}.GetItem(int)" />
    public T GetItem(int index)
        => _converter.Convert<T>(_info, index, requireValid: true)!;

    /// <inheritdoc cref="IDuckDbVector{T}.TryGetItem(int, out T)" />
    public bool TryGetItem(int index, [NotNullWhen(true)] out T? item)
        => _converter.TryConvert<T>(_info, index, out item);
}

/// <summary>
/// Extension methods on <see cref="DuckDbVectorReader{T}" /> and <see cref="DuckDbVectorRawReader{T}" />.
/// </summary>
/// <remarks>
/// These methods are extension methods rather than instance methods primarily 
/// so they can be precisely defined to apply to certain cases for the type parameter.
/// </remarks>
public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Retrieve one element from a DuckDB vector, allowing null values.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET value type for the element.
    /// </typeparam>
    /// <param name="vector">
    /// The vector to retrieve from.
    /// </param>
    /// <param name="index">
    /// The index of the desired element.
    /// </param>
    /// <returns>
    /// The value of the selected element, or null (if it is null in the DuckDB vector).
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public static T? GetNullableValue<T>(this in DuckDbVectorReader<T> vector, int index)
        where T : struct
        => vector.TryGetItem(index, out var item) ? item : null;

    /// <summary>
    /// Retrieve one element from a DuckDB vector, allowing null references.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET reference type for the element.
    /// </typeparam>
    /// <param name="vector">
    /// The vector to retrieve from.
    /// </param>
    /// <param name="index">
    /// The index of the desired element.
    /// </param>
    /// <returns>
    /// The value of the selected element, or null (if it is null in the DuckDB vector).
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    public static T? GetNullable<T>(this in DuckDbVectorReader<T> vector, int index)
        where T : class
        => vector.TryGetItem(index, out var item) ? item : null;
}
