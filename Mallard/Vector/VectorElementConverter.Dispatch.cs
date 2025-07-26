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
    /// Invoke a factory function, generically parameterized on <paramref name="type" />,
    /// that generates a <see cref="VectorElementConverter" />.
    /// </summary>
    /// <typeparam name="TArg">
    /// The type of the argument <paramref name="arg" /> to the factory function.
    /// </typeparam>
    /// <param name="method">
    /// A static method, with one generic parameter, 
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
    /// One or more types to substitute into the generic parameter of the method.
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
    /// For efficiency, the signature of <paramref name="method" /> is not checked
    /// in anyway.  It is simply assumed to follow the form described above.
    /// Violating that assumption will corrupt the .NET run-time; that is why this
    /// method is "unsafe".
    /// </para>
    /// </remarks>
    internal unsafe static VectorElementConverter
        UnsafeCreateFromGeneric<TArg>(MethodInfo method, TArg arg, ref readonly ConverterCreationContext context, params Type[] types)
    {
        ArgumentNullException.ThrowIfNull(method);

        var f = (delegate*<TArg, ref readonly ConverterCreationContext, VectorElementConverter>)
                    method.MakeGenericMethod(types).MethodHandle.GetFunctionPointer();
        return f(arg, in context);
    }

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
    /// The vector to convert items from.
    /// </param>
    /// <remarks>
    /// <para>
    /// The instance obtained must be created, and cannot be cached since this method
    /// has no context to do so.  
    /// </para>
    /// </remarks>
    internal unsafe static VectorElementConverter
        CreateForVectorUncached(Type? targetType, in DuckDbVectorInfo vector)
    {
        var context = new ConverterCreationContext(vector.ColumnInfo, vector.NativeVector);
        var converter = CreateForType(targetType, in context).BindToVector(vector);

        if (!converter.IsValid)
            DuckDbVectorInfo.ThrowForWrongParamType(vector.ValueKind, vector.StorageType, targetType ?? typeof(object));

        return converter;
    }

    /// <summary>
    /// Obtain an instance of <see cref="VectorElementConverter" />
    /// appropriate for the element type of a column and the desired .NET type.
    /// </summary>
    /// <param name="type">
    /// The desired .NET type to convert elements to. 
    /// This should not be a nullable value type.  (Null values are always handled outside
    /// of the converter function.)  If null, this method selects the most appropriate
    /// .NET type; this functionality is used to implement boxing conversions.
    /// The selected .NET type will be set in the <see cref="VectorElementConverter.TargetType" />
    /// of the return value.
    /// </param>
    /// <param name="context">
    /// Context for creating/obtaining a converter for the data in the DuckDB column
    /// in question.
    /// </param>
    /// <returns>
    /// A converter that works for the combination of <paramref name="type"/> and <paramref name="context" />,
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

            DuckDbValueKind.TinyInt when Match(type, typeof(sbyte)) => CreateForPrimitive<sbyte>(),
            DuckDbValueKind.SmallInt when Match(type, typeof(short)) => CreateForPrimitive<short>(),
            DuckDbValueKind.Integer when Match(type, typeof(int)) => CreateForPrimitive<int>(),
            DuckDbValueKind.BigInt when Match(type, typeof(long)) => CreateForPrimitive<long>(),

            DuckDbValueKind.UTinyInt when Match(type, typeof(byte)) => CreateForPrimitive<byte>(),
            DuckDbValueKind.USmallInt when Match(type, typeof(ushort)) => CreateForPrimitive<ushort>(),
            DuckDbValueKind.UInteger when Match(type, typeof(uint)) => CreateForPrimitive<uint>(),
            DuckDbValueKind.UBigInt when Match(type, typeof(ulong)) => CreateForPrimitive<ulong>(),

            DuckDbValueKind.Float when Match(type, typeof(float)) => CreateForPrimitive<float>(),
            DuckDbValueKind.Double when Match(type, typeof(double)) => CreateForPrimitive<double>(),

            DuckDbValueKind.Date when Match(type, typeof(DuckDbDate)) => CreateForPrimitive<DuckDbDate>(),
            DuckDbValueKind.Timestamp when Match(type, typeof(DuckDbTimestamp)) => CreateForPrimitive<DuckDbTimestamp>(),

            DuckDbValueKind.Interval when Match(type, typeof(DuckDbInterval)) => CreateForPrimitive<DuckDbInterval>(),

            DuckDbValueKind.VarChar when Match(type, typeof(string)) => DuckDbString.VectorElementConverter,
            DuckDbValueKind.VarInt when Match(type, typeof(BigInteger)) => DuckDbVarInt.VectorElementConverter,
            DuckDbValueKind.Bit when Match(type, typeof(BitArray)) => DuckDbBitString.VectorElementConverter,

            DuckDbValueKind.Blob when Match(type, typeof(byte[])) => DuckDbBlob.VectorElementConverter,

            DuckDbValueKind.UHugeInt when Match(type, typeof(UInt128)) => CreateForPrimitive<UInt128>(),
            DuckDbValueKind.HugeInt when Match(type, typeof(Int128)) => CreateForPrimitive<Int128>(),

            DuckDbValueKind.Decimal when Match(type, typeof(Decimal)) => DuckDbDecimal.GetVectorElementConverter(context.ColumnInfo),

            // N.B. This matches only T[] and not arbitrary System.Array objects
            // (with arbitrary ranks and lower/upper bounds)
            DuckDbValueKind.List when type == null || type.IsArray
                => ListConverter.ConstructForArray(type?.GetElementType(), in context),

            DuckDbValueKind.List when type.GetGenericUnderlyingType(typeof(ImmutableArray<>)) is Type elementType
                => ListConverter.ConstructForImmutableArray(elementType, in context),

            DuckDbValueKind.Enum when type != null && type.IsEnum => EnumConverter.CreateElementConverter(in context, type),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.UTinyInt
                                   && Match(type, typeof(byte)) => CreateForPrimitive<byte>(),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.USmallInt
                                   && Match(type, typeof(ushort)) => CreateForPrimitive<ushort>(),
            DuckDbValueKind.Enum when context.ColumnInfo.StorageKind == DuckDbValueKind.UInteger
                                   && Match(type, typeof(ulong)) => CreateForPrimitive<ulong>(),

            _ => default
        };
    }

    #endregion
}
