using System;
using System.Diagnostics;

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
/// While for built-in types, an indirect call is not necessary (and in fact 
/// <see cref="DuckDbVectorRawReader{T}" /> uses direct calls), 
/// it is necessary when dealing with nested
/// types (necessarily with non-null <see cref="_converterState" />).
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
internal unsafe readonly struct VectorElementConverter
{
    /// <summary>
    /// Opaque (cached) state to pass to <see cref="_function" />.
    /// </summary>
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
    ///     <description>Cached state used by the conversion function. </description>
    ///   </item>
    ///   <item>
    ///     <term><c>DuckDbVectorInfo*</c> <c>vector</c></term>
    ///     <description>Gives access to the DuckDB vector.  The caller
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
    /// </para>
    /// </remarks>
    private readonly void* _function;

    /// <summary>
    /// The .NET type to convert to.
    /// </summary>
    /// <remarks>
    /// Used to verify the consistent types are being passed in while being
    /// type-erased.
    /// </remarks>
    private readonly Type _type;

    private VectorElementConverter(object? state, void* function, Type type)
    {
        _state = state;
        _function = function;
        _type = type;
    }

    /// <summary>
    /// Create a pointer to a conversion function along with its state.
    /// </summary>
    /// <typeparam name="S">State object type for the conversion function. </typeparam>
    /// <typeparam name="T">The .NET type to convert to. </typeparam>
    public static VectorElementConverter
        Create<S,T>(S state, delegate*<S, in DuckDbVectorInfo, int, T> function)
        where S : class
        => new(state, function, typeof(T));

    /// <summary>
    /// Create a pointer to a stateless conversion function.
    /// </summary>
    /// <typeparam name="T">The .NET type to conver to. </typeparam>
    /// <remarks>
    /// When the conversion function is invoked, the first argument will be passed as null.
    /// </remarks>
    public static VectorElementConverter
        Create<T>(delegate*<object?, in DuckDbVectorInfo, int, T> function)
        => new(null, function, typeof(T));

    /// <summary>
    /// Safety wrapper to invoke the conversion function (through its pointer).  
    /// </summary>
    /// <typeparam name="T">
    /// The type to convert to.  This must exactly match the type on creation
    /// of this instance.
    /// </typeparam>
    public T Invoke<T>(in DuckDbVectorInfo vector, int index)
    {
        Debug.Assert(typeof(T) == _type,
            "Type passed to Invoke does not match that on creation of VectorElementConverter. ");

        var f = (delegate*<object?, in DuckDbVectorInfo, int, T>)_function;
        return f(_state, in vector, index);
    }

    /// <summary>
    /// Whether this instance specifies a valid converter (the function pointer is not null).
    /// </summary>
    public bool IsValid => _function != null;
}
