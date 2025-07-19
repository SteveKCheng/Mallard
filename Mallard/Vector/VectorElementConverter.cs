using Mallard.Basics;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Reflection;

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
        where T : notnull
        => new(state, function, typeof(T));

    /// <summary>
    /// Create a pointer to a stateless conversion function.
    /// </summary>
    /// <typeparam name="T">The .NET type to convert to. </typeparam>
    /// <remarks>
    /// When the conversion function is invoked, the first argument will be passed as null.
    /// </remarks>
    public static VectorElementConverter
        Create<T>(delegate*<object?, in DuckDbVectorInfo, int, T> function)
        where T : notnull
        => new(null, function, typeof(T));

    /// <summary>
    /// Safety wrapper to invoke the conversion function (through its pointer).  
    /// </summary>
    /// <typeparam name="T">
    /// The type to convert to.  This must exactly match the type on creation
    /// of this instance.
    /// </typeparam>
    public T Invoke<T>(in DuckDbVectorInfo vector, int index)
        where T : notnull
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

    #region Converters for primitive types (fixed-length, unmanaged that can be read directly from memory)

    /// <summary>
    /// Get the type converter that simply reads from the DuckDB vector's data block.
    /// </summary>
    /// <typeparam name="T">
    /// Unmanaged type compatible with the storage format of the DuckDB vector's elements.
    /// </typeparam>
    public static VectorElementConverter CreateForPrimitive<T>() where T : unmanaged
        => Create(&ReadPrimitive<T>);

    /// <summary>
    /// Read a "primitive" element, i.e. one whose memory representation in DuckDB is exactly
    /// the same as the .NET type <typeparamref name="T"/>.
    /// </summary>
    private static T ReadPrimitive<T>(object? state, in DuckDbVectorInfo vector, int index)
        where T : unmanaged
        => vector.UnsafeRead<T>(index);

    #endregion

    #region Dispatch for conversions of (generic) types

    /// <summary>
    /// Obtain an instance of <see cref="VectorElementConverter" />
    /// appropriate for the element type of the vector and the desired .NET type.
    /// </summary>
    /// <param name="type">
    /// The desired .NET type to convert elements to. 
    /// This should not be a nullable value type.  (Null values are always handled outside
    /// of the converter function.)
    /// </param>
    /// <param name="vector">
    /// Type information for the DuckDB vector.
    /// </param>
    /// <returns>
    /// A converter that works for the combination of <paramref name="type"/> and <paramref name="vector" />,
    /// or an invalid (default-initialized) instance if there is none suitable.
    /// </returns>
    public static VectorElementConverter
        CreateForType(Type type, in DuckDbVectorInfo vector)
    {
        return vector.BasicType switch
        {
            // Fortunately "bool" is considered an unmanaged type (of one byte), even though
            // P/Invoke marshalling does not treat it as such (because BOOL in the Win32 API is a 32-bit integer).
            // Strictly speaking, the C language does not define its "bool" (or "_Bool") type as one byte,
            // but common ABIs make it so, to be compatible with C++.
            DuckDbBasicType.Boolean when type == typeof(bool) => CreateForPrimitive<bool>(),

            DuckDbBasicType.TinyInt when type == typeof(sbyte) => CreateForPrimitive<sbyte>(),
            DuckDbBasicType.SmallInt when type == typeof(short) => CreateForPrimitive<short>(),
            DuckDbBasicType.Integer when type == typeof(int) => CreateForPrimitive<int>(),
            DuckDbBasicType.BigInt when type == typeof(long) => CreateForPrimitive<long>(),

            DuckDbBasicType.UTinyInt when type == typeof(byte) => CreateForPrimitive<byte>(),
            DuckDbBasicType.USmallInt when type == typeof(ushort) => CreateForPrimitive<ushort>(),
            DuckDbBasicType.UInteger when type == typeof(uint) => CreateForPrimitive<uint>(),
            DuckDbBasicType.UBigInt when type == typeof(ulong) => CreateForPrimitive<ulong>(),

            DuckDbBasicType.Float when type == typeof(float) => CreateForPrimitive<float>(),
            DuckDbBasicType.Double when type == typeof(double) => CreateForPrimitive<double>(),

            DuckDbBasicType.Date when type == typeof(DuckDbDate) => CreateForPrimitive<DuckDbDate>(),
            DuckDbBasicType.Timestamp when type == typeof(DuckDbTimestamp) => CreateForPrimitive<DuckDbTimestamp>(),

            DuckDbBasicType.Interval when type == typeof(DuckDbInterval) => CreateForPrimitive<DuckDbInterval>(),

            DuckDbBasicType.VarChar when type == typeof(string) => DuckDbString.Converter,

            DuckDbBasicType.UHugeInt when type == typeof(UInt128) => CreateForPrimitive<UInt128>(),
            DuckDbBasicType.HugeInt when type == typeof(Int128) => CreateForPrimitive<Int128>(),

            DuckDbBasicType.Decimal when type == typeof(Decimal) => DuckDbDecimal.CreateDecimalConverter(vector),

            DuckDbBasicType.List when type.IsInstanceOfGenericDefinition(typeof(ImmutableArray<>))
                => ListConverter.ConstructForImmutableArray(type, vector),
            // N.B. This matches only T[] and not arbitrary System.Array objects
            // (with arbitrary ranks and lower/upper bounds)
            DuckDbBasicType.List when type.IsArray => ListConverter.ConstructForArray(type, vector),

            _ => default
        };
    }

    /// <summary>
    /// Invoke a factory function, generically parameterized on <paramref name="type" />,
    /// that generates a <see cref="VectorElementConverter" />.
    /// </summary>
    /// <param name="method">
    /// A static method, with one generic parameter, 
    /// takes takes as its sole argument, <paramref name="vector" /> by read-only
    /// reference, and returns <see cref="VectorElementConverter" />.
    /// </param>
    /// <param name="type">
    /// The type to substitute into the generic parameter of the method.
    /// </param>
    /// <param name="vector">
    /// The DuckDB vector information to pass to the factory function.
    /// </param>
    /// <returns>
    /// The result of calling the factory function.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method is a tool to implement vector element converters when the element
    /// is a generic type or other composite type, where the type parameter cannot be
    /// extracted (completely) except by run-time reflection.  Then the code that
    /// does the processing with that type parameter needs to be instantiated 
    /// with reflection too.  This method encapsulates that logic.
    /// </para>
    /// <para>
    /// For efficiency, the signature of <paramref name="method" /> is not checked
    /// in anyway.  It is simply assumed to follow the form described above.
    /// Violating that assumption will corrupt the .NET run-time; that is why this
    /// method is "unsafe".
    /// </para>
    /// </remarks>
    internal unsafe static VectorElementConverter
        UnsafeCreateFromGeneric(MethodInfo method, Type type, in DuckDbVectorInfo vector)
    {
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(type);

        var f = (delegate*<in DuckDbVectorInfo, VectorElementConverter>)
                    method.MakeGenericMethod(type).MethodHandle.GetFunctionPointer();
        return f(vector);
    }

    #endregion
}
