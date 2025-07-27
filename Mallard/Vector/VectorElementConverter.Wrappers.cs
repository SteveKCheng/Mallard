using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Mallard;

internal readonly partial struct VectorElementConverter
{
    #region Boxing converters

    private static VectorElementConverter
        CreateForBoxedType(ref readonly ConverterCreationContext context)
    {
        var converter = context.ColumnInfo.ValueKind switch
        {
            DuckDbValueKind.Boolean => CreateForBoxedPrimitive<bool>(),

            DuckDbValueKind.TinyInt => CreateForBoxedPrimitive<sbyte>(),
            DuckDbValueKind.SmallInt => CreateForBoxedPrimitive<short>(),
            DuckDbValueKind.Integer => CreateForBoxedPrimitive<int>(),
            DuckDbValueKind.BigInt => CreateForBoxedPrimitive<long>(),

            DuckDbValueKind.UTinyInt => CreateForBoxedPrimitive<byte>(),
            DuckDbValueKind.USmallInt => CreateForBoxedPrimitive<ushort>(),
            DuckDbValueKind.UInteger => CreateForBoxedPrimitive<uint>(),
            DuckDbValueKind.UBigInt => CreateForBoxedPrimitive<ulong>(),

            DuckDbValueKind.Float => CreateForBoxedPrimitive<float>(),
            DuckDbValueKind.Double => CreateForBoxedPrimitive<double>(),

            DuckDbValueKind.Date => CreateForBoxedPrimitive<DuckDbDate>(),
            DuckDbValueKind.Timestamp => CreateForBoxedPrimitive<DuckDbTimestamp>(),

            DuckDbValueKind.Interval => CreateForBoxedPrimitive<DuckDbInterval>(),

            DuckDbValueKind.UHugeInt => CreateForBoxedPrimitive<UInt128>(),
            DuckDbValueKind.HugeInt => CreateForBoxedPrimitive<Int128>(),

            DuckDbValueKind.Decimal => DuckDbDecimal.GetBoxedVectorElementConverter(context.ColumnInfo),
            DuckDbValueKind.VarInt => DuckDbVarInt.BoxedVectorElementConverter,

            _ => default
        };

        // Above primitives have efficient implementations where the casting
        // is inlined into the conversion function.
        if (converter.IsValid)
        {
            Debug.Assert(!converter.TargetType.IsValueType);
            return converter;
        }

        // Decide on what the original (unboxed) type is first.
        converter = CreateForType(null, in context);

        // Nothing available, or the target type is a reference type so no boxing needed.
        if (!converter.IsValid || !converter.TargetType.IsValueType)
            return converter;

        Debug.Assert(!converter.TargetType.IsNullable());

        // Set up a second indirect call to box the return value 
        // from the original converter.
        return UnsafeCreateFromGeneric(CreateBoxingWrapperMethod,
                                       converter,
                                       in context,
                                       converter.TargetType);
    }

    private static readonly MethodInfo CreateBoxingWrapperMethod =
    typeof(BoxingConverter).GetMethod(nameof(BoxingConverter.Create),
                                      BindingFlags.Static | BindingFlags.NonPublic)!;

    /// <summary>
    /// Boxes the results of a vector element conversion that returns a value type, 
    /// when the client requests such.
    /// </summary>
    /// <remarks>
    /// Used internally by <see cref="VectorElementConverter.CreateForBoxedType(in DuckDbVectorInfo){T}" />
    /// when no more efficient alternative is available.
    /// </remarks>
    private sealed class BoxingConverter
    {
        private readonly VectorElementConverter _unboxedConverter;
        private BoxingConverter(VectorElementConverter unboxedConverter)
            => _unboxedConverter = unboxedConverter;
        private static object Convert<T>
            (BoxingConverter self, in DuckDbVectorInfo vector, int index) where T : struct
            => (object)self._unboxedConverter.UnsafeConvert<T>(vector, index);

        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        internal static unsafe VectorElementConverter Create<T>
            (VectorElementConverter unboxedConverter, ref readonly ConverterCreationContext _) where T : struct
            => VectorElementConverter.Create(new BoxingConverter(unboxedConverter), &Convert<T>);
    }

    #endregion

    #region Nullable wrapper

    private static VectorElementConverter
        CreateForNullableType(Type underlyingType, ref readonly ConverterCreationContext context)
    {
        var converter = context.ColumnInfo.ValueKind switch
        {
            DuckDbValueKind.Boolean when underlyingType == typeof(bool) => CreateForNullablePrimitive<bool>(),

            DuckDbValueKind.TinyInt when underlyingType == typeof(sbyte) => CreateForNullablePrimitive<sbyte>(),
            DuckDbValueKind.SmallInt when underlyingType == typeof(short) => CreateForNullablePrimitive<short>(),
            DuckDbValueKind.Integer when underlyingType == typeof(int) => CreateForNullablePrimitive<int>(),
            DuckDbValueKind.BigInt when underlyingType == typeof(long) => CreateForNullablePrimitive<long>(),

            DuckDbValueKind.UTinyInt when underlyingType == typeof(byte) => CreateForNullablePrimitive<byte>(),
            DuckDbValueKind.USmallInt when underlyingType == typeof(uint) => CreateForNullablePrimitive<ushort>(),
            DuckDbValueKind.UInteger when underlyingType == typeof(ulong) => CreateForNullablePrimitive<uint>(),
            DuckDbValueKind.UBigInt when underlyingType == typeof(ulong) => CreateForNullablePrimitive<ulong>(),

            DuckDbValueKind.Float when underlyingType == typeof(float) => CreateForNullablePrimitive<float>(),
            DuckDbValueKind.Double when underlyingType == typeof(double) => CreateForNullablePrimitive<double>(),

            DuckDbValueKind.Date when underlyingType == typeof(DuckDbDate) => CreateForNullablePrimitive<DuckDbDate>(),
            DuckDbValueKind.Timestamp when underlyingType == typeof(DuckDbTimestamp) => CreateForNullablePrimitive<DuckDbTimestamp>(),

            DuckDbValueKind.Interval when underlyingType == typeof(DuckDbInterval) => CreateForNullablePrimitive<DuckDbInterval>(),

            DuckDbValueKind.UHugeInt when underlyingType == typeof(UInt128) => CreateForNullablePrimitive<UInt128>(),
            DuckDbValueKind.HugeInt when underlyingType == typeof(Int128) => CreateForNullablePrimitive<Int128>(),

            DuckDbValueKind.Decimal when underlyingType == typeof(Decimal) => DuckDbDecimal.GetNullableVectorElementConverter(context.ColumnInfo),
            DuckDbValueKind.VarInt when underlyingType == typeof(BigInteger) => DuckDbVarInt.NullableVectorElementConverter,

            _ => default
        };

        // Above primitives have efficient implementations where the wrapping
        // in Nullable<T> is inlined into the conversion function.
        if (converter.IsValid)
        {
            Debug.Assert(Nullable.GetUnderlyingType(converter.TargetType) == underlyingType);
            return converter;
        }

        // Prepare to wrap generic wrapper around converter for original value type.
        converter = CreateForType(underlyingType, in context);

        // Nothing available.
        if (!converter.IsValid)
            return converter;

        Debug.Assert(converter.TargetType.IsValueType);

        // Set up a second indirect call to box the return value 
        // from the original converter.
        return UnsafeCreateFromGeneric(CreateNullableWrapperMethod,
                                       converter,
                                       in context,
                                       converter.TargetType);
    }

    private static readonly MethodInfo CreateNullableWrapperMethod =
    typeof(NullableConverter).GetMethod(nameof(BoxingConverter.Create),
                                        BindingFlags.Static | BindingFlags.NonPublic)!;

    /// <summary>
    /// Wraps the results of a vector element conversion that returns a value type,
    /// into a nullable.
    /// </summary>
    /// <remarks>
    /// Used internally by <see cref="VectorElementConverter.CreateForNullableType(Type, in DuckDbVectorInfo)" />
    /// when no more efficient alternative is available.
    /// </remarks>
    private sealed class NullableConverter
    {
        private readonly VectorElementConverter _underlyingConverter;
        private NullableConverter(VectorElementConverter underlyingConverter)
            => _underlyingConverter = underlyingConverter;
        private static T? Convert<T>
            (NullableConverter self, in DuckDbVectorInfo vector, int index) where T : struct
            => new Nullable<T>(self._underlyingConverter.UnsafeConvert<T>(vector, index));

        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        internal static unsafe VectorElementConverter Create<T>
            (VectorElementConverter underlyingConverter, ref readonly ConverterCreationContext _) where T : struct
            => VectorElementConverter.Create(new NullableConverter(underlyingConverter), &Convert<T>);
    }

    #endregion
}
