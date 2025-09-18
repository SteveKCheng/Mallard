using System;
using System.Collections;
using System.Collections.Immutable;
using System.Numerics;
using System.Reflection;

namespace Mallard;

internal readonly partial struct VectorElementConverter
{
    #region Dispatch for conversions of (generic) types

    /// <summary>
    /// Invoke a factory function, generically parameterized on <paramref name="types" />,
    /// that generates a <see cref="VectorElementConverter" />.
    /// </summary>
    /// <typeparam name="TArg">
    /// The type of the argument <paramref name="arg" /> to the factory function.
    /// </typeparam>
    /// <param name="method">
    /// A static method, with one or more generic parameters, 
    /// takes takes as two arguments, [1] <paramref name="arg" />,
    /// and [2] <paramref name="context" /> by read-only
    /// reference, and returns <see cref="VectorElementConverter" />.
    /// </param>
    /// <param name="arg">
    /// Arbitrary argument, of known type at compile-time, to pass to the factory function.
    /// </param>
    /// <param name="context">
    /// Context for constructing the converter for the desired DuckDB column.
    /// </param>
    /// <param name="types">
    /// One or more types to substitute into the generic parameters of the method.
    /// This argument is passed straight into 
    /// <see cref="MethodInfo.MakeGenericMethod(Type[])" />.
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
    /// For efficiency, in Release builds, the signature of <paramref name="method" /> is not checked
    /// in any way.  It is simply assumed to follow the form described above.
    /// Violating that assumption will corrupt the .NET run-time; that is why this
    /// method is "unsafe".
    /// </para>
    /// </remarks>
    internal unsafe static VectorElementConverter
        UnsafeCreateFromGeneric<TArg>(MethodInfo method, TArg arg, ref readonly ConverterCreationContext context, params Type[] types)
    {
        ArgumentNullException.ThrowIfNull(method);

        var instantiatedMethod = method.MakeGenericMethod(types);

#if DEBUG
        // To mitigate the danger of function pointers, in Debug mode we fully check the
        // argument types and return types.  It is probably faster, and certainly easier,
        // to create a delegate which implies those checks, then to do the checks manually
        // using .NET's reflection API.  We discard the result, since we still want to test
        // calling method in the same way as Release mode.
        _ = instantiatedMethod.CreateDelegate<CreateFromGenericTargetFunc<TArg>>();
#endif

        // Function pointers are more efficient but dangerous: the run-time cannot do
        // type-checking for us.  If any types mismatch then we would corrupt the run-time.
        // And unfortunately, it is rather easy to mess up the parameter types of the targeted 
        // method especially after re-factoring.
        var f = (delegate*<TArg, ref readonly ConverterCreationContext, VectorElementConverter>)
                    instantiatedMethod.MethodHandle.GetFunctionPointer();
        return f(arg, in context);
    }

#if DEBUG
    /// <summary>
    /// Dummy delegate used for type-checking function pointers (in Debug builds) 
    /// inside <see cref="UnsafeCreateFromGeneric" />.
    /// </summary>
    private delegate VectorElementConverter 
        CreateFromGenericTargetFunc<TArg>(TArg arg, ref readonly ConverterCreationContext context);
#endif

    /// <summary>
    /// Obtain an instance of <see cref="VectorElementConverter" /> appropriate
    /// for the element type of a vector and the desired .NET type.
    /// </summary>
    /// <param name="type">The desired .NET type to convert elements to. </param>
    /// <param name="typeMapping">Settings for type conversion coming from the user. </param>
    /// <param name="vector">The target vector. </param>
    /// <returns>
    /// Instance of <see cref="VectorElementConverter" /> that is closed for
    /// <paramref name="vector" />.
    /// </returns>
    /// <remarks>
    /// Helper used by <see cref="DuckDbVectorReader{T}.DuckDbVectorReader(in DuckDbVectorInfo)" />.
    /// This code is not integrated into that constructor only to avoid run-time duplication
    /// of code when the generic type is instantiated.
    /// </remarks>
    internal static VectorElementConverter
        CreateForVector(Type type, DuckDbTypeMapping typeMapping, in DuckDbVectorInfo vector)
    {
        var context = ConverterCreationContext.FromVector(vector, typeMapping);
        var converter = CreateForType(type, in context);

        if (!converter.IsValid)
            DuckDbVectorInfo.ThrowForWrongParamType(vector.ColumnInfo, type);

        return converter.BindToVector(vector);
    }

    /// <summary>
    /// Obtain an instance of <see cref="VectorElementConverter" />
    /// appropriate for the element type of a column and the desired .NET type.
    /// </summary>
    /// <param name="type">
    /// The desired .NET type to convert elements to. 
    /// If null, this method selects the most appropriate
    /// .NET type; this functionality is used to implement boxing conversions.
    /// The selected .NET type will be set in the <see cref="TargetType" /> property
    /// of the returned value.
    /// </param>
    /// <param name="context">
    /// Context for creating/obtaining a converter for the data in the DuckDB column
    /// in question.
    /// </param>
    /// <returns>
    /// A converter that works for the combination of <paramref name="type" /> and <paramref name="context" />,
    /// or an invalid (default-initialized) instance if there is none suitable.  If the conversion
    /// function requires binding to a specific vector, the returned instance is unbound.
    /// </returns>
    public static VectorElementConverter
        CreateForType(Type? type, ref readonly ConverterCreationContext context)
    {
        // Request for boxing.  Note that CreateForBoxedType may recursively call this
        // function (with type == null) to get the converter for the unboxed type first.
        if (type == typeof(object))
            return CreateForBoxedType(in context);

        // Nullable types handled through a separate dispatch.
        if (type != null && (Nullable.GetUnderlyingType(type) is Type underlyingType))
            return CreateForNullableType(underlyingType, in context);

        // Allow matching against a null (unknown) type
        static bool Match(Type? type, Type target)
            => type == null || type == target;

        return context.ColumnInfo.ValueKind switch
        {
            // Fortunately "bool" is considered an unmanaged type (of one byte), even though
            // P/Invoke marshalling does not treat it as such (because BOOL in the Win32 API is a 32-bit integer).
            // Strictly speaking, the C language does not define its "bool" (or "_Bool") type as one byte,
            // but common ABIs make it so, to be compatible with C++.
            DuckDbValueKind.Boolean when Match(type, typeof(bool)) => CreateForPrimitive<bool>(),

            // Signed integers
            DuckDbValueKind.TinyInt when Match(type, typeof(sbyte)) => CreateForPrimitive<sbyte>(),
            DuckDbValueKind.SmallInt when Match(type, typeof(short)) => CreateForPrimitive<short>(),
            DuckDbValueKind.Integer when Match(type, typeof(int)) => CreateForPrimitive<int>(),
            DuckDbValueKind.BigInt when Match(type, typeof(long)) => CreateForPrimitive<long>(),
            DuckDbValueKind.HugeInt when Match(type, typeof(Int128)) => CreateForPrimitive<Int128>(),

            // Unsigned integers
            DuckDbValueKind.UTinyInt when Match(type, typeof(byte)) => CreateForPrimitive<byte>(),
            DuckDbValueKind.USmallInt when Match(type, typeof(ushort)) => CreateForPrimitive<ushort>(),
            DuckDbValueKind.UInteger when Match(type, typeof(uint)) => CreateForPrimitive<uint>(),
            DuckDbValueKind.UBigInt when Match(type, typeof(ulong)) => CreateForPrimitive<ulong>(),
            DuckDbValueKind.UHugeInt when Match(type, typeof(UInt128)) => CreateForPrimitive<UInt128>(),

            // Promoted signed integers
            DuckDbValueKind.TinyInt when IsPromotedIntegralType<sbyte>(type) => CreateForCastedInteger<sbyte>(type),
            DuckDbValueKind.SmallInt when IsPromotedIntegralType<short>(type) => CreateForCastedInteger<short>(type),
            DuckDbValueKind.Integer when IsPromotedIntegralType<int>(type) => CreateForCastedInteger<int>(type),
            DuckDbValueKind.BigInt when IsPromotedIntegralType<long>(type) => CreateForCastedInteger<long>(type),
            DuckDbValueKind.HugeInt when IsPromotedIntegralType<Int128>(type) => CreateForCastedInteger<Int128>(type),

            // Promoted unsigned integers
            DuckDbValueKind.UTinyInt when IsPromotedIntegralType<byte>(type) => CreateForCastedInteger<byte>(type),
            DuckDbValueKind.USmallInt when IsPromotedIntegralType<ushort>(type) => CreateForCastedInteger<ushort>(type),
            DuckDbValueKind.UInteger when IsPromotedIntegralType<uint>(type) => CreateForCastedInteger<uint>(type),
            DuckDbValueKind.UBigInt when IsPromotedIntegralType<ulong>(type) => CreateForCastedInteger<ulong>(type),
            DuckDbValueKind.UHugeInt when IsPromotedIntegralType<UInt128>(type) => CreateForCastedInteger<UInt128>(type),

            // Binary floating-point numbers
            DuckDbValueKind.Float when Match(type, typeof(float)) => CreateForPrimitive<float>(),
            DuckDbValueKind.Double when Match(type, typeof(double)) => CreateForPrimitive<double>(),

            // Date and times
            DuckDbValueKind.Date when type == typeof(DateOnly) || (type == null && !context.ConvertDatesAsDateTime) => CreateFor<DuckDbDate, DateOnly>(),
            DuckDbValueKind.Date when type == typeof(DateTime) || (type == null &&  context.ConvertDatesAsDateTime) => CreateFor<DuckDbDate, DateTime>(),
            DuckDbValueKind.Date when type == typeof(DuckDbDate) => CreateForPrimitive<DuckDbDate>(),
            DuckDbValueKind.Timestamp when Match(type, typeof(DuckDbTimestamp)) => CreateForPrimitive<DuckDbTimestamp>(),
            DuckDbValueKind.Time when type == typeof(DuckDbTime) => CreateForPrimitive<DuckDbTime>(),
            DuckDbValueKind.Time when Match(type, typeof(TimeOnly)) => CreateFor<DuckDbTime, TimeOnly>(),

            DuckDbValueKind.Interval when Match(type, typeof(DuckDbInterval)) => CreateForPrimitive<DuckDbInterval>(),

            // Other numbers
            DuckDbValueKind.VarInt when Match(type, typeof(BigInteger)) => CreateFor<DuckDbVarInt, BigInteger>(),
            DuckDbValueKind.Decimal when Match(type, typeof(Decimal)) => DuckDbDecimal.GetConverterForDecimal(context.ColumnInfo),
            DuckDbValueKind.Decimal when type == typeof(DuckDbDecimal) => DuckDbDecimal.GetConverterForDuckDbDecimal(context.ColumnInfo),

            // UUIDs
            DuckDbValueKind.Uuid when Match(type, typeof(Guid)) => CreateFor<DuckDbUuid, Guid>(),
            DuckDbValueKind.Uuid when type == typeof(DuckDbUuid) => CreateForPrimitive<DuckDbUuid>(),
            DuckDbValueKind.Uuid when type == typeof(UInt128) => CreateForPrimitive<UInt128>(),

            // Variable-length types excluding generic containers
            DuckDbValueKind.VarChar when Match(type, typeof(string)) => CreateFor<DuckDbString, string>(),
            DuckDbValueKind.Bit when Match(type, typeof(BitArray)) => CreateFor<DuckDbBitString, BitArray>(),
            DuckDbValueKind.Blob when Match(type, typeof(byte[])) => CreateFor<DuckDbBlob, byte[]>(),

            // N.B. This matches only T[] and not arbitrary System.Array objects
            // (with arbitrary ranks and lower/upper bounds)
            DuckDbValueKind.List when type == null || type.IsArray
                => ListConverter.ConstructForArray(type?.GetElementType(), in context),

            DuckDbValueKind.List when type.GetGenericUnderlyingType(typeof(ImmutableArray<>)) is Type elementType
                => ListConverter.ConstructForImmutableArray(elementType, in context),

            DuckDbValueKind.Struct when Match(type, typeof(DuckDbStruct)) => StructConverter.GetConverter(in context),

            // Enumerations
            DuckDbValueKind.Enum when type != null && type.IsEnum => EnumConverter.CreateElementConverter(in context, type),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.UTinyInt
                                   && Match(type, typeof(byte)) => CreateForPrimitive<byte>(),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.USmallInt
                                   && Match(type, typeof(ushort)) => CreateForPrimitive<ushort>(),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.UInteger
                                   && Match(type, typeof(ulong)) => CreateForPrimitive<ulong>(),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.UTinyInt
                                   && IsPromotedIntegralType<byte>(type) => CreateForCastedInteger<byte>(type),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.USmallInt
                                   && IsPromotedIntegralType<ushort>(type) => CreateForCastedInteger<ushort>(type),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.UInteger
                                   && IsPromotedIntegralType<uint>(type) => CreateForCastedInteger<uint>(type),

            _ => default
        };
    }

    #endregion
}
