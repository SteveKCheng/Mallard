using System;

namespace Mallard;

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