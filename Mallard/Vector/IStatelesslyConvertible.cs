using System;

namespace Mallard;

/// <summary>
/// Standard signature used to simplify implementations of stateless conversions
/// of items from a DuckDB vector to a .NET type. 
/// </summary>
/// <typeparam name="TSource">
/// The C-compatible type that the elements of the vector are presented in 
/// the native memory allocated and filled by DuckDB.
/// </typeparam>
/// <typeparam name="TTarget">
/// The .NET type to convert to.
/// </typeparam>
/// <remarks>
/// <para>
/// Writing converters in this form (particularly when <typeparamref name="TTarget" /> is a struct)
/// allows the converters for boxing and nullables to be generated without manual (and error-prone) 
/// duplication of code, or run-time reflection.  Because the .NET IL compiler monomorphizes generic methods when 
/// value types are substituted in for generic parameters, the resulting machine code will be 
/// efficient with no indirect calls through function pointers or delegates.  
/// (This is a technique that is well-known for decades in C++, 
/// and recently enabled in .NET with "static abstract" methods in interfaces.)
/// </para>
/// </remarks>
internal interface IStatelesslyConvertible<TSource, TTarget> 
    where TSource : unmanaged, allows ref struct
    where TTarget : notnull
{
    /// <summary>
    /// Convert an element of a DuckDB vector to a .NET value.
    /// </summary>
    /// <param name="item">
    /// The item in the vector, in DuckDB's native representation.
    /// </param>
    /// <returns>
    /// The item in the vector converted to the desired .NET representation.
    /// </returns>
    static abstract TTarget Convert(ref readonly TSource item);
}

internal readonly partial struct VectorElementConverter
{
    /// <summary>
    /// Encapsulate a stateless converter for a vector element from DuckDB 
    /// that is implemented in the standard way.
    /// </summary>
    /// <typeparam name="TSource">
    /// The C-compatible type that the elements of the vector are presented in 
    /// the native memory allocated and filled by DuckDB.
    /// </typeparam>
    /// <typeparam name="TTarget">
    /// The .NET type to convert to.
    /// </typeparam>
    internal static unsafe VectorElementConverter CreateFor<TSource, TTarget>() 
        where TSource : unmanaged, IStatelesslyConvertible<TSource, TTarget>, allows ref struct
        where TTarget : notnull
    {
        static TTarget ReadAndConvert(object? state, in DuckDbVectorInfo vector, int index)
            => TSource.Convert(in vector.UnsafeRead<TSource>(index));

        return Create(&ReadAndConvert);
    }

    /// <summary>
    /// Create a nullable wrapper for a vector element that results in a value type in .NET.
    /// </summary>
    /// <typeparam name="TSource">
    /// The C-compatible type that the elements of the vector are presented in 
    /// the native memory allocated and filled by DuckDB.
    /// </typeparam>
    /// <typeparam name="TTarget">
    /// The underlying type in .NET (behind the <see cref="Nullable{T}" />) of the vector element after converter.
    /// </typeparam>
    internal static unsafe VectorElementConverter CreateNullableFor<TSource, TTarget>()
        where TSource : unmanaged, IStatelesslyConvertible<TSource, TTarget>, allows ref struct
        where TTarget : struct
    {
        static TTarget? ReadAndConvert(object? state, in DuckDbVectorInfo vector, int index)
            => new Nullable<TTarget>(TSource.Convert(in vector.UnsafeRead<TSource>(index)));

        return Create(&ReadAndConvert);
    }

    /// <summary>
    /// Create a boxing wrapper for a vector element that results in a value type in .NET.
    /// </summary>
    /// <typeparam name="TSource">
    /// The C-compatible type that the elements of the vector are presented in 
    /// the native memory allocated and filled by DuckDB.
    /// </typeparam>
    /// <typeparam name="TTarget">
    /// The underlying type in .NET (behind the box) of the vector element after converter.
    /// </typeparam>
    internal static unsafe VectorElementConverter CreateBoxingFor<TSource, TTarget>()
        where TSource : unmanaged, IStatelesslyConvertible<TSource, TTarget>, allows ref struct
        where TTarget : struct
    {
        static object ReadAndConvert(object? state, in DuckDbVectorInfo vector, int index)
            => (object)TSource.Convert(in vector.UnsafeRead<TSource>(index));

        return Create(&ReadAndConvert);
    }
}
