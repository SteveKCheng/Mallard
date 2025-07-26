using System;

namespace Mallard;

/// <summary>
/// Generates new state objects for vector element conversion when they are needed
/// to operate on new vectors.
/// </summary>
/// <typeparam name="TState">
/// The type of state behind the implementation function of <see cref="VectorElementConverter" />.
/// </typeparam>
/// <remarks>
/// <para>
/// Converters for nested types like lists rely on state objects that are specific
/// to the DuckDB vector being converted.  That is, those objects embed <see cref="DuckDbVectorInfo" />
/// representing the "children" vectors of the list-valued vector, or other such nested vectors.
/// </para>
/// <para>
/// Unfortunately, that means the state objects cannot be cached across chunks and re-used for
/// converting vectors for the same column.  But such state objects are precisely the ones that
/// are expensive to construct from scratch because they need to analyze the logical type of the column
/// from DuckDB.  This interface allows the analysis of the type of column to be encapsulated,
/// in an implementating object of this interface, and then new state objects can be manufactured
/// to deal with the vector of the same column, each chunk.
/// </para>
/// <para>
/// New GC objects still need to be created for each chunk, but at least less computation will
/// be required.  (We cannot re-use the object because chunks from the same DuckDB result 
/// can be represented independently through multiple instances of <see cref="DuckDbResultChunk" />. 
/// The only consolation is that nested types occur comparatively rarely, to primitive types
/// (which all can be stateless and fast).
/// </para>
/// </remarks>
internal interface IConverterBinder<out TState> where TState : class
{
    /// <summary>
    /// Manufacture the state object for conversion (as passed to the implementation function
    /// to <see cref="VectorElementConverter" />) on the given DuckDB vector.
    /// </summary>
    /// <param name="vector">
    /// The DuckDB vector to bind to the stateful converter to.
    /// </param>
    TState BindToVector(in DuckDbVectorInfo vector);
}

internal unsafe readonly partial struct VectorElementConverter
{
    #region Rebinding stateful converters to new vectors of the same type

    /// <summary>
    /// Whether the implementation of vector element conversion is stateful, 
    /// and requires binding to the vector before it can be invoked.
    /// </summary>
    public bool RequiresBinding => _binder != null;

    /// <summary>
    /// Whether this converter is "closed", i.e. it has been bound to a vector already,
    /// or the converter state does not need binding.
    /// </summary>
    /// <remarks>
    /// The terminology "closed" is in analogy with delegates being closed
    /// (versus "open" delegates wrapping instance methods where the instance object has
    /// not yet been supplied).
    /// </remarks>
    public bool IsClosed => !RequiresBinding || _state != null;

    /// <summary>
    /// Encapsulate a conversion function that requires vector-specific state.
    /// </summary>
    /// <typeparam name="S">State object type for the conversion function. </typeparam>
    /// <typeparam name="T">The .NET type to convert to. </typeparam>
    /// <param name="binder">
    /// Object that creates instances of <typeparamref name="S" /> when this
    /// converter is bound via <see cref="BindToVector(in DuckDbVectorInfo)" />.
    /// </param>
    /// <param name="function">
    /// Implementation function of the conversion.  Its first argument will always
    /// be the result of calling <see cref="IConverterBinder{S}.BindToVector(in DuckDbVectorInfo)" />
    /// on <paramref name="binder" />.
    /// </param>
    /// <param name="defaultValueIsInvalid">
    /// The desired value of <see cref="DefaultValueIsInvalid" />.  Ignored if <typeparamref name="T" />
    /// is a reference type or is nullable.
    /// </param>
    public static VectorElementConverter
        Create<S, T>(IConverterBinder<S> binder, delegate*<S, in DuckDbVectorInfo, int, T> function, bool defaultValueIsInvalid = false)
        where S : class
      => new(null,
             function,
             typeof(T),
             !typeof(T).IsValueType || typeof(T).IsNullable() || defaultValueIsInvalid,
             binder);

    /// <summary>
    /// Return a new "closed" version of this instance, after binding to the given vector.
    /// </summary>
    /// <param name="vector">
    /// The DuckDB vector to bind to.
    /// </param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException">
    /// This instance requires vector-specific binding but it has already been bound.
    /// </exception>
    public VectorElementConverter BindToVector(in DuckDbVectorInfo vector)
    {
        if (_binder == null)
            return this;

        if (_state != null)
            throw new InvalidOperationException("Cannot bind a converter to a vector when it is already bound. ");

        var newState = _binder.BindToVector(vector);
        return new(newState, _function, TargetType, DefaultValueIsInvalid, _binder);
    }

    #endregion
}
