using System;

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
}
