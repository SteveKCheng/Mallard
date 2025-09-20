using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Reflection;

namespace Mallard;
using Mallard.Types;

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

            DuckDbValueKind.Date when !context.ConvertDatesAsDateTime => CreateBoxingFor<DuckDbDate,DateOnly>(),
            DuckDbValueKind.Date when  context.ConvertDatesAsDateTime => CreateBoxingFor<DuckDbDate,DateTime>(),
            DuckDbValueKind.Timestamp => CreateForBoxedPrimitive<DuckDbTimestamp>(),

            DuckDbValueKind.Interval => CreateForBoxedPrimitive<DuckDbInterval>(),

            DuckDbValueKind.UHugeInt => CreateForBoxedPrimitive<UInt128>(),
            DuckDbValueKind.HugeInt => CreateForBoxedPrimitive<Int128>(),

            DuckDbValueKind.Decimal => DuckDbDecimal.GetConverterForBoxedDecimal(context.ColumnInfo),
            DuckDbValueKind.VarInt => CreateBoxingFor<DuckDbBigInteger, BigInteger>(),

            DuckDbValueKind.Uuid => CreateBoxingFor<DuckDbUuid, Guid>(),

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
        return UnsafeCreateFromGeneric(CreateBoxingConverterMethod,
                                       converter,
                                       in context,
                                       converter.TargetType);
    }

    private static readonly MethodInfo CreateBoxingConverterMethod =
    typeof(ConverterWrapper).GetMethod(nameof(ConverterWrapper.CreateBoxingConverter),
                                       BindingFlags.Static | BindingFlags.NonPublic)!;

    #endregion

    #region Converters to nullable

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

            DuckDbValueKind.Date when underlyingType == typeof(DateOnly) => CreateNullableFor<DuckDbDate, DateOnly>(),
            DuckDbValueKind.Date when underlyingType == typeof(DateTime) => CreateNullableFor<DuckDbDate, DateTime>(),
            DuckDbValueKind.Date when underlyingType == typeof(DuckDbDate) => CreateForNullablePrimitive<DuckDbDate>(),
            DuckDbValueKind.Timestamp when underlyingType == typeof(DuckDbTimestamp) => CreateForNullablePrimitive<DuckDbTimestamp>(),

            DuckDbValueKind.Interval when underlyingType == typeof(DuckDbInterval) => CreateForNullablePrimitive<DuckDbInterval>(),

            DuckDbValueKind.UHugeInt when underlyingType == typeof(UInt128) => CreateForNullablePrimitive<UInt128>(),
            DuckDbValueKind.HugeInt when underlyingType == typeof(Int128) => CreateForNullablePrimitive<Int128>(),

            DuckDbValueKind.Decimal when underlyingType == typeof(Decimal) => DuckDbDecimal.GetConverterForNullableDecimal(context.ColumnInfo),
            DuckDbValueKind.Decimal when underlyingType == typeof(DuckDbDecimal) => DuckDbDecimal.GetConverterForNullableDuckDbDecimal(context.ColumnInfo),
            DuckDbValueKind.VarInt when underlyingType == typeof(BigInteger) => CreateNullableFor<DuckDbBigInteger, BigInteger>(),

            // UUIDs
            DuckDbValueKind.Uuid when underlyingType == typeof(Guid) => CreateNullableFor<DuckDbUuid, Guid>(),
            DuckDbValueKind.Uuid when underlyingType == typeof(DuckDbUuid) => CreateForNullablePrimitive<DuckDbUuid>(),
            DuckDbValueKind.Uuid when underlyingType == typeof(UInt128) => CreateForNullablePrimitive<UInt128>(),

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
        return UnsafeCreateFromGeneric(CreateNullableConverterMethod,
                                       converter,
                                       in context,
                                       converter.TargetType);
    }

    private static readonly MethodInfo CreateNullableConverterMethod =
    typeof(ConverterWrapper).GetMethod(nameof(ConverterWrapper.CreateNullableConverter),
                                       BindingFlags.Static | BindingFlags.NonPublic)!;

    #endregion

    #region State object common to the boxing and nullable wrappers

    /// <summary>
    /// Invokes a vector element conversion that returns a value type, and then wraps the value
    /// in a box or <see cref="Nullable{T}" />.
    /// </summary>
    /// <remarks>
    /// Used internally by <see cref="VectorElementConverter.CreateForBoxedType" />
    /// and <see cref="VectorElementConverter.CreateForNullableType" />
    /// when no more efficient alternative is available.
    /// </remarks>
    private sealed class ConverterWrapper
    {
        private readonly VectorElementConverter _underlyingConverter;

        private ConverterWrapper(VectorElementConverter underlyingConverter)
            => _underlyingConverter = underlyingConverter;

        private static object ConvertAndWrapInBox<T>
            (ConverterWrapper self, in DuckDbVectorInfo vector, int index) where T : struct
            => (object)self._underlyingConverter.UnsafeConvert<T>(vector, index);

        private static Nullable<T> ConvertAndWrapInNullable<T>
            (ConverterWrapper self, in DuckDbVectorInfo vector, int index) where T : struct
            => new Nullable<T>(self._underlyingConverter.UnsafeConvert<T>(vector, index));

        private class Binder(VectorElementConverter underlyingConverterUnbound) : IConverterBinder<ConverterWrapper>
        {
            private readonly VectorElementConverter _underlyingConverterUnbound = underlyingConverterUnbound;
            public ConverterWrapper BindToVector(in DuckDbVectorInfo vector)
                => new(_underlyingConverterUnbound.BindToVector(vector));
        }

        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        internal static unsafe VectorElementConverter CreateBoxingConverter<T>
            (VectorElementConverter underlyingConverter, ref readonly ConverterCreationContext _) where T : struct
            => underlyingConverter.RequiresBinding
                ? VectorElementConverter.Create(new Binder(underlyingConverter), &ConvertAndWrapInBox<T>)
                : VectorElementConverter.Create(new ConverterWrapper(underlyingConverter), &ConvertAndWrapInBox<T>);

        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
        internal static unsafe VectorElementConverter CreateNullableConverter<T>
            (VectorElementConverter underlyingConverter, ref readonly ConverterCreationContext _) where T : struct
            => underlyingConverter.RequiresBinding
                ? VectorElementConverter.Create(new Binder(underlyingConverter), &ConvertAndWrapInNullable<T>)
                : VectorElementConverter.Create(new ConverterWrapper(underlyingConverter), &ConvertAndWrapInNullable<T>);
    }

    #endregion
}
