using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Mallard;

/// <summary>
/// Specifies a converter, to an instance of a .NET type, for an element of a DuckDB vector.
/// </summary>
/// <remarks>
/// <para>
/// A function pointer is used instead of a virtual method to avoid
/// allocating objects in simple cases (not involving nested/composite types).
/// </para>
/// <para>
/// While for built-in types an indirect call is not necessary (and in fact 
/// <see cref="DuckDbVectorRawReader{T}" /> uses direct calls), 
/// it is necessary when dealing with nested
/// types (necessarily with non-null <see cref="_state" />).
/// Of course, the flexibility of indirect call allows us to add user-customizable
/// conversions later on.
/// </para>
/// <para>
/// Despite the indirect call, we try very hard to maintain strong typing, i.e.
/// we do not cast to <see cref="System.Object" /> (unless specifically asked
/// by the user).  This is important when reading large amounts of data.
/// </para>
/// <para>
/// Yet, to reduce the level of run-time reflection necessary to implement
/// nested converters --- which is important for ahead-of-time compilation ---
/// the function pointer is <i>type-erased</i> (to <c>void*</c>),
/// and then cast back afterwards.  This makes the implementation 
/// rather "unsafe", but the ultimate goal is still to make the public API
/// safe.
/// </para>
/// </remarks>
internal unsafe readonly partial struct VectorElementConverter
{
    #region Data

    /// <summary>
    /// Opaque pre-computed state to pass to <see cref="_function" />.
    /// </summary>
    /// <remarks>
    /// This member is null if the converter implementation is stateless, or
    /// it requires vector-specific binding but has not been bound yet.
    /// </remarks>
    private readonly object? _state;

    /// <summary>
    /// Pointer to function that reads an element of the vector and converts it to 
    /// some .NET type <c>T</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The arguments to the function are as follows:
    /// </para>
    /// <list type="table">
    ///   <listheader>
    ///     <term>Parameter</term>  
    ///     <description>Description</description>
    ///   </listheader>
    ///   <item>
    ///     <term><c>object?</c> <c>state</c></term>
    ///     <description>Pre-computed state used by the conversion function. </description>
    ///   </item>
    ///   <item>
    ///     <term><c>DuckDbVectorInfo*</c> <c>vector</c></term>
    ///     <description>Gives access to the DuckDB vector.  The callee
    ///     must access the native data with the correct type.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <term><c>int</c> <c>index</c></term>
    ///     <description>The index of the desired element of the vector. 
    ///     The callee may assume the index refers to a valid element. 
    ///     The caller is responsible for checking the index is valid
    ///     and refers to a valid (non-null) element.
    ///     </description>
    ///   </item>
    /// </list>
    /// <para>
    /// The function shall return the element type <c>T</c>.
    /// When <c>T</c> is a reference type or a nullable value type,
    /// the return value shall not be null.
    /// </para>
    /// </remarks>
    private readonly void* _function;

    /// <summary>
    /// The .NET type to convert to.
    /// </summary>
    /// <remarks>
    /// Used to verify the consistent types are being passed in while being
    /// type-erased.  It may also be consulted if the target type is unknown
    /// because the user requested all values be boxed to objects. 
    /// </remarks>
    public Type TargetType { get; }

    /// <summary>
    /// Whether the default value of <see cref="TargetType" /> is considered
    /// to mark an invalid state.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Converters for a composite type (e.g. an array) may consult this flag to suppress 
    /// exceptions if individual items within an instance of the composite type are invalid.  
    /// <see cref="VectorElementConverter" /> itself does not do anything with this flag.
    /// </para>
    /// <para>
    /// The "default" value refers to the default initialization in .NET of a variable
    /// of that type.
    /// </para>
    /// <para>
    /// This flag is automatically true for reference types and nullable value types.
    /// </para>
    /// </remarks>
    public bool DefaultValueIsInvalid { get; }

    /// <summary>
    /// Whether this instance specifies a valid converter (the function pointer is not null).
    /// </summary>
    public bool IsValid => _function != null;

    /// <summary>
    /// Binder for converter states that are vector-specific.
    /// </summary>
    private readonly IConverterBinder<object>? _binder;

    #endregion

    #region Constructors

    private VectorElementConverter(object? state, 
                                   void* function, 
                                   Type targetType, 
                                   bool defaultValueIsInvalid,
                                   IConverterBinder<object>? rebinder = null)
    {
        _state = state;
        _function = function;
        TargetType = targetType;
        DefaultValueIsInvalid = defaultValueIsInvalid;
        _binder = rebinder;
    }

    /// <summary>
    /// Encapsulate a conversion function along with its state.
    /// </summary>
    /// <typeparam name="S">State object type for the conversion function. </typeparam>
    /// <typeparam name="T">The .NET type to convert to. </typeparam>
    /// <param name="state">
    /// The state object to pass in when invoking <paramref name="function" />.
    /// This state object must not be specific to any vector (for the same DuckDB column), as it may be cached
    /// for re-use across chunks.  To use vector-specific states, use 
    /// the overload of this method that takes <see cref="IConverterBinder{TState}" /> instead.
    /// </param>
    /// <param name="function">
    /// Implementation function of the conversion.  It will only be passed
    /// <paramref name="state" /> as its first argument.
    /// </param>
    /// <param name="defaultValueIsInvalid">
    /// The desired value of <see cref="DefaultValueIsInvalid" />.  Ignored if <typeparamref name="T" />
    /// is a reference type or is nullable.
    /// </param>
    public static VectorElementConverter
        Create<S,T>(S state, delegate*<S, in DuckDbVectorInfo, int, T> function, bool defaultValueIsInvalid = false)
        where S : class
        => new(state, function, typeof(T), !typeof(T).IsValueType || typeof(T).IsNullable() || defaultValueIsInvalid);

    /// <summary>
    /// Encapsulate a stateless conversion function.
    /// </summary>
    /// <typeparam name="T">The .NET type to convert to. </typeparam>
    /// <param name="function">
    /// Implementation function of the conversion.  Its first argument will always be passed as null
    /// (because it has no state).
    /// </param>
    /// <param name="defaultValueIsInvalid">
    /// The desired value of <see cref="DefaultValueIsInvalid" />.  Ignored if <typeparamref name="T" />
    /// is a reference type or is nullable.
    /// </param>
    public static VectorElementConverter
        Create<T>(delegate*<object?, in DuckDbVectorInfo, int, T> function, bool defaultValueIsInvalid = false)
        => new(null, function, typeof(T), !typeof(T).IsValueType || typeof(T).IsNullable() || defaultValueIsInvalid);

    #endregion

    #region Executing conversions

    /// <summary>
    /// Invoke the converter to convert an element from a DuckDB vector.
    /// </summary>
    /// <typeparam name="T">
    /// The type to convert to.  This must exactly match the type on creation
    /// of this instance.
    /// </typeparam>
    /// <param name="vector">The DuckDB vector to read from. </param>
    /// <param name="index">The index of the element within the vector. </param>
    /// <param name="result">Stores the converted result, or the default value
    /// if the element is (marked) invalid in the vector.
    /// </param>
    /// <exception cref="IndexOutOfRangeException">
    /// <paramref name="index" /> is out of range for the vector.
    /// </exception>
    /// <returns>
    /// Whether the element in the vector is valid.  (Even when <typeparamref name="T" />
    /// is a nullable type, validity is reported with respect to the DuckDB vector,
    /// not with respect to the .NET type.)
    /// </returns>
    public bool TryConvert<T>(in DuckDbVectorInfo vector, int index, [NotNullWhen(true)] out T? result)        
    {
        Debug.Assert(typeof(T).IsAssignableWithoutBoxingFrom(TargetType),
            "The type passed to TryConvert is not compatible with the type that this VectorElementConverter was created for. ");

        if (vector.IsItemValid(index))
        {
            var f = (delegate*<object?, in DuckDbVectorInfo, int, T>)_function;
            result = f(_state, in vector, index)!;
            Debug.Assert(typeof(T).IsValueType || (object)result != null,
                        "Converter function returned a null object. ");
            return true;
        }
        else
        {
            result = default;
            return false;
        }
    }

    /// <summary>
    /// Invoke the converter to convert an element from a DuckDB vector,
    /// possibly throwing an exception if the element does not exist.
    /// </summary>
    /// <typeparam name="T">
    /// The type to convert to.  This must be ABI-compatible with the type on creation
    /// of this instance.  Caution: this condition is only checked in debug mode!
    /// User-facing interfaces/methods require either explicit run-time checks 
    /// or else the API surface must be designed to be statically type-safe
    /// (i.e. no type erasure).
    /// </typeparam>
    /// <param name="vector">The DuckDB vector to read from. </param>
    /// <param name="index">The index of the element within the vector. </param>
    /// <param name="requireValid">
    /// If true, throw an exception if the element is invalid in the DuckDB vector,
    /// and the <typeparamref name="T" /> is not some <see cref="Nullable{U}" />.
    /// If false, the default value for <typeparamref name="T" /> is returned
    /// when the element is invalid.  (When <typeparamref name="T" />
    /// is <see cref="Nullable{U}" />, this default value is just the "null" value.)
    /// </param>
    /// <returns>
    /// The converted element, or the default value for <typeparamref name="T" /> 
    /// if the element is invalid in the DuckDB vector.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T? Convert<T>(in DuckDbVectorInfo vector, int index, bool requireValid)
    {
        bool isValid = TryConvert<T>(vector, index, out var result);
        if (!isValid && requireValid && !typeof(T).IsNullable())
            DuckDbVectorInfo.ThrowForInvalidElement(index);
        return result;
    }

    /// <summary>
    /// Same as <see cref="Convert{T}(in DuckDbVectorInfo, int, bool)" />
    /// but does not checking whatsoever.
    /// </summary>
    /// <remarks>
    /// Used by the boxing and nullable-value wrappers.
    /// The check for a valid vector index and element should already have been
    /// executed when the wrapper gets invoked.
    /// </remarks>
    /// <typeparam name="T">
    /// Value type to return (and to be wrapped).
    /// </typeparam>
    /// <param name="vector">The vector to read the element from. </param>
    /// <param name="index">An index for the vector that is in range
    /// and refers to a valid element. </param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private T UnsafeConvert<T>(in DuckDbVectorInfo vector, int index) where T : struct
    {
        var f = (delegate*<object?, in DuckDbVectorInfo, int, T>)_function;
        return f(_state, in vector, index)!;
    }

    #endregion
}
