using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Mallard;

internal readonly partial struct VectorElementConverter
{
    #region Boxing converters

    private static VectorElementConverter
        CreateForBoxedType(in DuckDbVectorInfo vector)
    {
        var converter = vector.ValueKind switch
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

            DuckDbValueKind.Decimal => DuckDbDecimal.GetBoxedVectorElementConverter(vector.ColumnInfo),
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
        converter = CreateForType(null, vector);

        // Nothing available, or the target type is a reference type so no boxing needed.
        if (!converter.IsValid || !converter.TargetType.IsValueType)
            return converter;

        Debug.Assert(!converter.TargetType.IsNullable());

        // Set up a second indirect call to box the return value 
        // from the original converter.
        return UnsafeCreateFromGeneric(CreateBoxingWrapperMethod,
                                       converter,
                                       vector,
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
            (VectorElementConverter unboxedConverter, in DuckDbVectorInfo _) where T : struct
            => VectorElementConverter.Create(new BoxingConverter(unboxedConverter), &Convert<T>);
    }

    #endregion

    #region Nullable wrapper

    private static VectorElementConverter
        CreateForNullableType(Type underlyingType, in DuckDbVectorInfo vector)
    {
        var converter = vector.ValueKind switch
        {
            DuckDbValueKind.Boolean => CreateForNullablePrimitive<bool>(),

            DuckDbValueKind.TinyInt => CreateForNullablePrimitive<sbyte>(),
            DuckDbValueKind.SmallInt => CreateForNullablePrimitive<short>(),
            DuckDbValueKind.Integer => CreateForNullablePrimitive<int>(),
            DuckDbValueKind.BigInt => CreateForNullablePrimitive<long>(),

            DuckDbValueKind.UTinyInt => CreateForNullablePrimitive<byte>(),
            DuckDbValueKind.USmallInt => CreateForNullablePrimitive<ushort>(),
            DuckDbValueKind.UInteger => CreateForNullablePrimitive<uint>(),
            DuckDbValueKind.UBigInt => CreateForNullablePrimitive<ulong>(),

            DuckDbValueKind.Float => CreateForNullablePrimitive<float>(),
            DuckDbValueKind.Double => CreateForNullablePrimitive<double>(),

            DuckDbValueKind.Date => CreateForNullablePrimitive<DuckDbDate>(),
            DuckDbValueKind.Timestamp => CreateForNullablePrimitive<DuckDbTimestamp>(),

            DuckDbValueKind.Interval => CreateForNullablePrimitive<DuckDbInterval>(),

            DuckDbValueKind.UHugeInt => CreateForNullablePrimitive<UInt128>(),
            DuckDbValueKind.HugeInt => CreateForNullablePrimitive<Int128>(),

            DuckDbValueKind.Decimal => DuckDbDecimal.GetNullableVectorElementConverter(vector.ColumnInfo),
            DuckDbValueKind.VarInt => DuckDbVarInt.NullableVectorElementConverter,

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
        converter = CreateForType(underlyingType, vector);

        // Nothing available.
        if (!converter.IsValid)
            return converter;

        Debug.Assert(converter.TargetType.IsValueType);

        // Set up a second indirect call to box the return value 
        // from the original converter.
        return UnsafeCreateFromGeneric(CreateNullableWrapperMethod,
                                       converter,
                                       vector,
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
            (VectorElementConverter underlyingConverter, in DuckDbVectorInfo _) where T : struct
            => VectorElementConverter.Create(new NullableConverter(underlyingConverter), &Convert<T>);
    }

    #endregion
}
