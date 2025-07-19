using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
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
    /// type-erased.  It may also be consulted if the target type is unknown
    /// because the user requested all values be boxed to objects. 
    /// </remarks>
    public Type TargetType { get; init; }

    private VectorElementConverter(object? state, void* function, Type targetType)
    {
        _state = state;
        _function = function;
        TargetType = targetType;
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
        Debug.Assert(typeof(T).IsValueType ? typeof(T) == TargetType 
                                           : typeof(T).IsAssignableFrom(TargetType),
            "Type passed to Invoke is not compatible with the type that this VectorElementConverter was created for. ");

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
    /// of the converter function.)  If null, this method selects the most appropriate
    /// .NET type; this functionality is used to implement boxing conversions.
    /// The selected .NET type will be set in the <see cref="VectorElementConverter.TargetType" />
    /// of the return value.
    /// </param>
    /// <param name="vector">
    /// Type information for the DuckDB vector.
    /// </param>
    /// <returns>
    /// A converter that works for the combination of <paramref name="type"/> and <paramref name="vector" />,
    /// or an invalid (default-initialized) instance if there is none suitable.
    /// </returns>
    public static VectorElementConverter
        CreateForType(Type? type, in DuckDbVectorInfo vector)
    {
        // Request for boxing.  Note that CreateForBoxedType may recursively call this
        // function (with type == null) to get the converter for the unboxed type first.
        if (type == typeof(object))
            return CreateForBoxedType(vector);

        // Allow matching against a null (unknown) type
        static bool Match(Type? type, Type target)
            => type == null || type == target;
 
        return vector.BasicType switch
        {
            // Fortunately "bool" is considered an unmanaged type (of one byte), even though
            // P/Invoke marshalling does not treat it as such (because BOOL in the Win32 API is a 32-bit integer).
            // Strictly speaking, the C language does not define its "bool" (or "_Bool") type as one byte,
            // but common ABIs make it so, to be compatible with C++.
            DuckDbBasicType.Boolean when Match(type, typeof(bool)) => CreateForPrimitive<bool>(),

            DuckDbBasicType.TinyInt when Match(type, typeof(sbyte)) => CreateForPrimitive<sbyte>(),
            DuckDbBasicType.SmallInt when Match(type, typeof(short)) => CreateForPrimitive<short>(),
            DuckDbBasicType.Integer when Match(type, typeof(int)) => CreateForPrimitive<int>(),
            DuckDbBasicType.BigInt when Match(type, typeof(long)) => CreateForPrimitive<long>(),

            DuckDbBasicType.UTinyInt when Match(type, typeof(byte)) => CreateForPrimitive<byte>(),
            DuckDbBasicType.USmallInt when Match(type, typeof(ushort)) => CreateForPrimitive<ushort>(),
            DuckDbBasicType.UInteger when Match(type, typeof(uint)) => CreateForPrimitive<uint>(),
            DuckDbBasicType.UBigInt when Match(type, typeof(ulong)) => CreateForPrimitive<ulong>(),

            DuckDbBasicType.Float when Match(type, typeof(float)) => CreateForPrimitive<float>(),
            DuckDbBasicType.Double when Match(type, typeof(double)) => CreateForPrimitive<double>(),

            DuckDbBasicType.Date when Match(type, typeof(DuckDbDate)) => CreateForPrimitive<DuckDbDate>(),
            DuckDbBasicType.Timestamp when Match(type, typeof(DuckDbTimestamp)) => CreateForPrimitive<DuckDbTimestamp>(),

            DuckDbBasicType.Interval when Match(type, typeof(DuckDbInterval)) => CreateForPrimitive<DuckDbInterval>(),

            DuckDbBasicType.VarChar when Match(type, typeof(string)) => DuckDbString.Converter,

            DuckDbBasicType.UHugeInt when Match(type, typeof(UInt128)) => CreateForPrimitive<UInt128>(),
            DuckDbBasicType.HugeInt when Match(type, typeof(Int128)) => CreateForPrimitive<Int128>(),

            DuckDbBasicType.Decimal when Match(type, typeof(Decimal)) => DuckDbDecimal.CreateDecimalConverter(vector),

            DuckDbBasicType.List when type == null => ListConverter.ConstructForArrayOfUnknownType(vector),

            DuckDbBasicType.List when type.IsInstanceOfGenericDefinition(typeof(ImmutableArray<>))
                => ListConverter.ConstructForImmutableArray(type, vector),
            // N.B. This matches only T[] and not arbitrary System.Array objects
            // (with arbitrary ranks and lower/upper bounds)
            DuckDbBasicType.List when type != null && type.IsArray => ListConverter.ConstructForArray(type, vector),

            _ => default
        };
    }

    /// <summary>
    /// Invoke a factory function, generically parameterized on <paramref name="type" />,
    /// that generates a <see cref="VectorElementConverter" />.
    /// </summary>
    /// <typeparam name="TArg">
    /// The type of the argument <paramref name="arg" /> to the factory function.
    /// </typeparam>
    /// <param name="method">
    /// A static method, with one generic parameter, 
    /// takes takes as two arguments, [1] <paramref name="arg" />,
    /// and [2] <paramref name="vector" /> by read-only
    /// reference, and returns <see cref="VectorElementConverter" />.
    /// </param>
    /// <param name="type">
    /// The type to substitute into the generic parameter of the method.
    /// </param>
    /// <param name="arg">
    /// Arbitrary argument, of known type at compile-time, to pass to the factory function.
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
        UnsafeCreateFromGeneric<TArg>(MethodInfo method, Type type, TArg arg, in DuckDbVectorInfo vector)
    {
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(type);

        var f = (delegate*<TArg, in DuckDbVectorInfo, VectorElementConverter>)
                    method.MakeGenericMethod(type).MethodHandle.GetFunctionPointer();
        return f(arg, vector);
    }

    #endregion

    #region Boxing converters

    public static VectorElementConverter CreateForBoxedPrimitive<T>() where T : unmanaged
        => Create(&ReadPrimitiveAndBox<T>);

    private static object ReadPrimitiveAndBox<T>(object? state, in DuckDbVectorInfo vector, int index)
        where T : unmanaged
        => (object)vector.UnsafeRead<T>(index);

    private static VectorElementConverter
        CreateForBoxedType(in DuckDbVectorInfo vector)
    {
        var converter = vector.BasicType switch
        {
            DuckDbBasicType.Boolean => CreateForBoxedPrimitive<bool>(),

            DuckDbBasicType.TinyInt => CreateForBoxedPrimitive<sbyte>(),
            DuckDbBasicType.SmallInt => CreateForBoxedPrimitive<short>(),
            DuckDbBasicType.Integer => CreateForBoxedPrimitive<int>(),
            DuckDbBasicType.BigInt => CreateForBoxedPrimitive<long>(),

            DuckDbBasicType.UTinyInt => CreateForBoxedPrimitive<byte>(),
            DuckDbBasicType.USmallInt => CreateForBoxedPrimitive<ushort>(),
            DuckDbBasicType.UInteger => CreateForBoxedPrimitive<uint>(),
            DuckDbBasicType.UBigInt => CreateForBoxedPrimitive<ulong>(),

            DuckDbBasicType.Float => CreateForBoxedPrimitive<float>(),
            DuckDbBasicType.Double => CreateForBoxedPrimitive<double>(),

            DuckDbBasicType.Date => CreateForBoxedPrimitive<DuckDbDate>(),
            DuckDbBasicType.Timestamp => CreateForBoxedPrimitive<DuckDbTimestamp>(),

            DuckDbBasicType.Interval => CreateForBoxedPrimitive<DuckDbInterval>(),

            DuckDbBasicType.UHugeInt => CreateForBoxedPrimitive<UInt128>(),
            DuckDbBasicType.HugeInt => CreateForBoxedPrimitive<Int128>(),

            _ => default
        };

        // Above primitives have efficient implementations where the casting
        // is inlined into the conversion function.
        if (converter.IsValid)
            return converter;

        // Decide on what the original (unboxed) type first.
        converter = CreateForType(null, vector);

        // Nothing available, or the target type is a reference type so no boxing needed.
        if (!converter.IsValid || !converter.TargetType.IsValueType)
            return converter;

        // Set up a second indirect call to box the return value 
        // from the original converter.
        return UnsafeCreateFromGeneric(CreateBoxingWrapperMethod, 
                                       converter.TargetType, 
                                       converter, 
                                       vector); 
    }

    private static readonly MethodInfo CreateBoxingWrapperMethod =
    typeof(BoxingConverter).GetMethod(nameof(BoxingConverter.Create),
                                      BindingFlags.Static | BindingFlags.NonPublic)!;

    /// <summary>
    /// Boxes the results of a vector element conversion that returns a value type, 
    /// when the client requests such.
    /// </summary>
    /// <remarks>
    /// Used internally by <see cref="VectorElementConverter.CreateForBoxedPrimitive{T}" />
    /// when no more efficient alternative is available.
    /// </remarks>
    internal sealed class BoxingConverter
    {
        private readonly VectorElementConverter _unboxedConverter;
        private BoxingConverter(VectorElementConverter unboxedConverter)
            => _unboxedConverter = unboxedConverter;
        private static object Convert<T>
            (BoxingConverter self, in DuckDbVectorInfo vector, int index) where T : struct
            => (object)self._unboxedConverter.Invoke<T>(vector, index);

        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        internal static unsafe VectorElementConverter Create<T>
            (VectorElementConverter unboxedConverter, in DuckDbVectorInfo _) where T : struct
            => VectorElementConverter.Create(new BoxingConverter(unboxedConverter), &Convert<T>);
    }

    #endregion
}
