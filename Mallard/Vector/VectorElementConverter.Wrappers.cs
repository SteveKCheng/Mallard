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

            DuckDbBasicType.Decimal => DuckDbDecimal.GetBoxedVectorElementConverter(vector),

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
    /// Used internally by <see cref="VectorElementConverter.CreateForBoxedPrimitive{T}" />
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
}
