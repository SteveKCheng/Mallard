using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;

namespace Mallard;

internal readonly partial struct VectorElementConverter
{
    // This hack depends on an assumption which is not entirely justifiable.
    // While we match the enum type with its storage type, that does not necessarily
    // mean that functions returning the enum type have the same ABI, in the .NET CLR
    // internally, as functions returning the storage type.  And the .NET CLR might
    // have run-time checks that the wrong type of functions are not being called.
    //
    // To do this casting properly, we would need a wrapper function that calls
    // Unsafe.BitCast.  But that means use of run-time reflection to be able
    // to instantiate the generic function over an arbitrary enum type.
    internal unsafe VectorElementConverter CastReturnTypeToEnum(Type enumType)
    {
        Debug.Assert(Enum.GetUnderlyingType(enumType) == TargetType);
        return new VectorElementConverter(_state, _function, enumType, DefaultValueIsInvalid);
    }
}

internal sealed class EnumConverter
{
    private readonly DuckDbEnumDictionary _enumDict;
    private readonly string[] _clrMemberNames;
    private readonly Array _clrMemberValues;
    private readonly FrozenDictionary<string, int> _nameDict;
    private readonly Type _clrType;

    private EnumConverter(DuckDbEnumDictionary enumDict, Type clrType)
    {
        _enumDict = enumDict;
        _clrMemberNames = Enum.GetNames(clrType);
        _clrMemberValues = Enum.GetValuesAsUnderlyingType(clrType);
        _clrType = clrType;

        static IEnumerable<KeyValuePair<string, int>> EnumerateNamesAndIndices(string[] memberNames)
        {
            for (int i = 0; i < memberNames.Length; ++i)
                yield return new KeyValuePair<string, int>(memberNames[i], i);
        }

        _nameDict = FrozenDictionary.ToFrozenDictionary(EnumerateNamesAndIndices(_clrMemberNames));
    }

    private EnumConverter(scoped in DuckDbVectorInfo vector, Type clrType)
        : this(DuckDbEnumDictionary.CreateFromVector(vector), clrType)
    {
    }

    private static TStorage ConvertElement<TSource, TStorage>(EnumConverter self, in DuckDbVectorInfo vector, int index)
        where TSource : unmanaged, IBinaryInteger<TSource>
        where TStorage : unmanaged
    {
        var clrMemberValues = (TStorage[])self._clrMemberValues;

        var nativeValue = vector.UnsafeRead<TSource>(index);
        uint nativeValueUInt = uint.CreateTruncating(nativeValue);

        var memberName = self._enumDict[nativeValueUInt];

        if (self._nameDict.TryGetValue(memberName, out var clrMemberIndex))
            return clrMemberValues[clrMemberIndex];

        throw new InvalidOperationException(
            $"The enumeration member {memberName} (value = {nativeValue}) does not have a " +
            $"corresponding member in the .NET enumeration type {self._clrType.FullName}. ");
    }

    private unsafe static VectorElementConverter CreateStage2<TSource, TStorage>(in DuckDbVectorInfo vector, Type enumType)
        where TSource : unmanaged, IBinaryInteger<TSource>
        where TStorage : unmanaged
    {
        var state = new EnumConverter(vector, enumType);
        return VectorElementConverter.Create(state, &ConvertElement<TSource, TStorage>)
                                     .CastReturnTypeToEnum(enumType);
    }

    private static VectorElementConverter CreateStage1<TSource>(in DuckDbVectorInfo vector, Type enumType)
        where TSource : unmanaged, IBinaryInteger<TSource>
    {
        var underlyingType = Enum.GetUnderlyingType(enumType);
        if (underlyingType == typeof(byte)) return CreateStage2<TSource, byte>(vector, enumType);
        if (underlyingType == typeof(sbyte)) return CreateStage2<TSource, sbyte>(vector, enumType);
        if (underlyingType == typeof(short)) return CreateStage2<TSource, short>(vector, enumType);
        if (underlyingType == typeof(ushort)) return CreateStage2<TSource, ushort>(vector, enumType);
        if (underlyingType == typeof(int)) return CreateStage2<TSource, int>(vector, enumType);
        if (underlyingType == typeof(uint)) return CreateStage2<TSource, uint>(vector, enumType);
        if (underlyingType == typeof(long)) return CreateStage2<TSource, int>(vector, enumType);
        if (underlyingType == typeof(ulong)) return CreateStage2<TSource, ulong>(vector, enumType);
        throw new NotSupportedException("Underlying type of .NET enumeration is not supported for conversion. ");
    }

    internal static VectorElementConverter CreateElementConverter(in DuckDbVectorInfo vector, Type enumType)
        => vector.StorageType switch
           {
               DuckDbBasicType.UTinyInt => CreateStage1<byte>(vector, enumType),
               DuckDbBasicType.USmallInt => CreateStage1<ushort>(vector, enumType),
               DuckDbBasicType.UInteger => CreateStage1<uint>(vector, enumType),
               _ => throw new InvalidOperationException("Cannot decode enumeration from a DuckDB vector with the given storage type. ")
           };
}
