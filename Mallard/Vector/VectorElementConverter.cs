namespace Mallard;

/// <summary>
/// Specifies a converter, to an instance of a .NET type, for an element of a DuckDB vector.
/// </summary>
/// <typeparam name="T">
/// The .NET type to convert to.
/// </typeparam>
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
/// </remarks>
internal unsafe readonly struct VectorElementConverter<T>
{
    /// <summary>
    /// Opaque (cached) state to pass to <see cref="_function" />.
    /// </summary>
    private readonly object? _state;

    /// <summary>
    /// Pointer to function that reads an element of the vector and converts it to 
    /// type <typeparamref name="T" />.
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
    ///     <description>Information on the vector.  This argument is passed by
    ///     pointer rather than through <c>in</c> (read-only reference) 
    ///     only to work around a limitation of C#
    ///     where reference arguments in function pointers cannot be marked <c>scoped</c>;
    ///     without restricting the scope the code will not compile.
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
    /// </remarks>
    private readonly delegate*<object?, DuckDbVectorInfo*, int, T> _function;

    private VectorElementConverter(object? state, delegate*<object?, DuckDbVectorInfo*, int, T> function)
    {
        _state = state;
        _function = function;
    }

    /// <summary>
    /// Create a pointer to a conversion function along with its state.
    /// </summary>
    /// <typeparam name="U"></typeparam>
    /// <remarks>
    /// <para>
    /// This factory method is a work around for a limitation in .NET's generics;
    /// otherwise we would just use a plain constructor.
    /// </para>
    /// <para>
    /// When trying to "specialize" the code in this class on a specific type 
    /// <typeparamref name="U" /> (e.g. <c>string</c>) conditional on type tests, 
    /// the C# compiler and the .NET system deduce that the code that falls under
    /// the condition has <typeparamref name="T" /> equal to <typeparamref name="U" />,
    /// and therefore will not allow an assignment of the function pointer for
    /// conversion of type <typeparamref name="U" /> into a function pointer for
    /// type <typeparamref name="T" />.  We use this method to cast function pointers
    /// so the code can compile.  Naturally, <typeparamref name="U" /> must be
    /// dynamically equal to <typeparamref name="T" /> or else the .NET runtime
    /// system would be corrupted.
    /// </para>
    /// </remarks>
    public static VectorElementConverter<T> 
        Create<U>(object? state, delegate*<object?, DuckDbVectorInfo*, int, U> function)
        => new(state, (delegate*<object?, DuckDbVectorInfo*, int, T>)function);

    /// <summary>
    /// Safety wrapper to invoke the conversion function (through its pointer).  
    /// </summary>
    public T Invoke(in DuckDbVectorInfo vector, int index)
    {
        // A ref struct cannot be in GC memory, but nevertheless C# requires
        // its member to be "fixed" (even though it must be a no-op).
        fixed (DuckDbVectorInfo* vectorPtr = &vector)
            return _function(_state, vectorPtr, index);
    }

    /// <summary>
    /// Whether this instance specifies a valid converter (the function pointer is not null).
    /// </summary>
    public bool IsValid => _function != null;
}
