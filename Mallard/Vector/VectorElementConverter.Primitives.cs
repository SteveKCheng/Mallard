using System;
using System.Numerics;

namespace Mallard;

internal readonly partial struct VectorElementConverter
{
    #region Converters for primitive types (fixed-length, unmanaged that can be read directly from memory)

    /// <summary>
    /// Get the type converter that simply reads a primitive value 
    /// from the DuckDB vector's data block.
    /// </summary>
    /// <typeparam name="T">
    /// Unmanaged type whose memory representation matches exactly the element type
    /// of the DuckDB vector.
    /// </typeparam>
    private unsafe static VectorElementConverter CreateForPrimitive<T>() where T : unmanaged
    {
        static T ReadItem(object? state, in DuckDbVectorInfo vector, int index) 
            => vector.UnsafeRead<T>(index);
        return Create(&ReadItem);
    }

    /// <summary>
    /// Get the type converter that reads a primitive value from a DuckDB vector,
    /// then boxes it to <see cref="object" />.
    /// </summary>
    /// <typeparam name="T">
    /// Unmanaged type whose memory representation matches exactly the element type
    /// of the DuckDB vector.
    /// </typeparam>
    /// <remarks>
    /// Primitive types occur commonly enough that we compile efficient boxing functions for them
    /// rather than wrapping the unboxed function generically.
    /// </remarks>
    private unsafe static VectorElementConverter CreateForBoxedPrimitive<T>() where T : unmanaged
    {
        static object ReadItemAndBox(object? state, in DuckDbVectorInfo vector, int index) 
            => (object)vector.UnsafeRead<T>(index);
        return Create(&ReadItemAndBox);
    }

    /// <summary>
    /// Get the type converter that reads a primitive value from a DuckDB vector,
    /// then wraps it in <see cref="Nullable{T}" />.
    /// </summary>
    /// <typeparam name="T">
    /// Unmanaged type whose memory representation matches exactly the element type
    /// of the DuckDB vector.
    /// </typeparam>
    /// <remarks>
    /// Primitive types occur commonly enough that we compile efficient nullable wrappers for them.
    /// </remarks>
    private unsafe static VectorElementConverter CreateForNullablePrimitive<T>() where T : unmanaged
    {
        static T? ReadItemAndWrap(object? state, in DuckDbVectorInfo vector, int index)
            => new Nullable<T>(vector.UnsafeRead<T>(index));
        return Create(&ReadItemAndWrap);
    }

    #endregion

    #region Reading integers with promotion of types

    /// <summary>
    /// Get the type converter that reads a primitive integer value, and casts it to another
    /// primitive integer value.
    /// </summary>
    /// <remarks>
    /// The cast is not checked (for overflow/underflow) during conversion.  It is intended that
    /// the target type is always bigger than the source type.
    /// </remarks>
    /// <typeparam name="TSource">
    /// The integer type present as elements of the DuckDB vector.
    /// </typeparam>
    /// <typeparam name="TTarget">
    /// The integer type to convert to.
    /// </typeparam>
    private unsafe static VectorElementConverter CreateForCastedInteger<TSource, TTarget>() 
        where TSource : unmanaged, IBinaryInteger<TSource>
        where TTarget : IBinaryInteger<TTarget>
    {
        static TTarget ReadItemAndCast(object? state, in DuckDbVectorInfo vector, int index)
            => TTarget.CreateTruncating(vector.UnsafeRead<TSource>(index));
        return Create(&ReadItemAndCast);
    }

    /// <summary>
    /// Get the type converter that reads a primitive integer value, and casts it to another
    /// primitive integer value.
    /// </summary>
    /// <remarks>
    /// The cast is not checked (for overflow/underflow) during conversion.  It is intended that
    /// the target type is always bigger than the source type.
    /// </remarks>
    /// <typeparam name="TSource">
    /// The integer type present as elements of the DuckDB vector.
    /// </typeparam>
    /// <param name="type">
    /// The integer type to convert to.
    /// </param>
    /// <exception cref="NotSupportedException">
    /// The conversion from the source type to the target type is not possible.
    /// </exception>
    private static VectorElementConverter CreateForCastedInteger<TSource>(Type? type)
        where TSource : unmanaged, IBinaryInteger<TSource>
    {
        if (type == typeof(sbyte)) return CreateForCastedInteger<TSource, sbyte>();
        if (type == typeof(short)) return CreateForCastedInteger<TSource, short>();
        if (type == typeof(int)) return CreateForCastedInteger<TSource, int>();
        if (type == typeof(long)) return CreateForCastedInteger<TSource, long>();
        if (type == typeof(Int128)) return CreateForCastedInteger<TSource, Int128>();
        if (type == typeof(byte)) return CreateForCastedInteger<TSource, byte>();
        if (type == typeof(ushort)) return CreateForCastedInteger<TSource, ushort>();
        if (type == typeof(uint)) return CreateForCastedInteger<TSource, uint>();
        if (type == typeof(ulong)) return CreateForCastedInteger<TSource, ulong>();
        if (type == typeof(UInt128)) return CreateForCastedInteger<TSource, UInt128>();
        throw new NotSupportedException();
    }

    /// <summary>
    /// Rank the built-in integral types for determining if one can be promoted
    /// to another without data loss.
    /// </summary>
    private static (int Rank, bool IsSigned) CategorizeIntegralType(Type? type)
    {
        if (type == typeof(sbyte)) return (sizeof(sbyte), true);
        if (type == typeof(short)) return (sizeof(short), true);
        if (type == typeof(int)) return (sizeof(int), true);
        if (type == typeof(long)) return (sizeof(long), true);
        if (type == typeof(Int128)) return (2 * sizeof(long), true);
        if (type == typeof(byte)) return (sizeof(byte), false);
        if (type == typeof(ushort)) return (sizeof(ushort), false);
        if (type == typeof(uint)) return (sizeof(uint), false);
        if (type == typeof(ulong)) return (sizeof(ulong), false);
        if (type == typeof(UInt128)) return (2 * sizeof(ulong), false);
        return (-1, false);
    }

    /// <summary>
    /// Test for integer promotion possibilities when converting to an integral type.
    /// </summary>
    /// <typeparam name="TSource">The source integer type. </typeparam>
    /// <param name="targetType">The conversion target type.  </param>
    /// <returns>
    /// True if <paramref name="targetType" /> is an integral type that 
    /// can hold all values of the type <typeparamref name="TSource" />, and has the
    /// same signedness.  False otherwise.
    /// </returns>
    private static bool IsPromotedIntegralType<TSource>(Type? targetType) where TSource : IBinaryInteger<TSource>
    {
        var (sourceRank, sourceSignedness) = CategorizeIntegralType(typeof(TSource));
        var (targetRank, targetSignedness) = CategorizeIntegralType(targetType);
        return sourceRank >= 0 && sourceRank <= targetRank && sourceSignedness == targetSignedness;
    }

    #endregion
}
