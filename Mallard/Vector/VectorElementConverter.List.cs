using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Mallard;

/// <summary>
/// State object required to convert a DuckDB list and its contents to
/// a .NET array or array-like type.
/// </summary>
internal sealed class ListConverter
{
    /// <summary>
    /// Vector for the list children.
    /// </summary>
    private readonly DuckDbVectorInfo _childrenInfo;

    /// <summary>
    /// Type converter for the list children.
    /// </summary>
    private readonly VectorElementConverter _childrenConverter;

    private static readonly MethodInfo ConstructForArrayImplMethod =
        typeof(ListConverter).GetMethod(nameof(ConstructForArrayImpl),
                                        BindingFlags.Static | BindingFlags.NonPublic)!;

    private static readonly MethodInfo ConstructForImmutableArrayImplMethod =
        typeof(ListConverter).GetMethod(nameof(ConstructForImmutableArrayImpl), 
                                        BindingFlags.Static | BindingFlags.NonPublic)!;

    private ListConverter(Type? childType, in DuckDbVectorInfo parent)
    {
        _childrenInfo = parent.GetChildrenVectorInfo();
        _childrenConverter = VectorElementConverter.CreateForType(childType, _childrenInfo);
        if (!_childrenConverter.IsValid)
        {
            throw new ArgumentException(
                childType != null 
                    ? $"The element type of the list/array cannot be converted to .NET type {childType}. "
                    : "The element type of the list/array cannot be converted to any .NET type. ");
        }
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForArrayImpl<T>(ListConverter self, in DuckDbVectorInfo parent)
        => VectorElementConverter.Create(self, &ConvertToArray<T>);

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForImmutableArrayImpl<T>(ListConverter self, in DuckDbVectorInfo parent)
        => VectorElementConverter.Create(self, &ConvertToImmutableArray<T>, defaultValueIsInvalid: true);

    public static VectorElementConverter ConstructForArray(Type? listType, in DuckDbVectorInfo parent)
    {
        Type? childType = null;

        if (listType != null)
        {
            if (!listType.IsArray)
                throw new ArgumentException("The target type must be an array. ", nameof(listType));

            childType = listType.GetElementType()!;
        }

        var self = new ListConverter(childType, parent);
        childType = self._childrenConverter.TargetType;

        return VectorElementConverter.UnsafeCreateFromGeneric(ConstructForArrayImplMethod, 
                                                              self, parent, childType);
    }

    public static VectorElementConverter ConstructForImmutableArray(Type listType, in DuckDbVectorInfo parent)
    {
        if (!listType.IsInstanceOfGenericDefinition(typeof(ImmutableArray<>)))
            throw new ArgumentException("The target type must be ImmutableArray<T>. ", nameof(listType));

        var childType = listType.GetGenericArguments()[0];
        var self = new ListConverter(childType, parent);

        return VectorElementConverter.UnsafeCreateFromGeneric(ConstructForImmutableArrayImplMethod, 
                                                              self, parent, childType);
    }

    private T? ConvertChild<T>(int childIndex)
        => _childrenConverter.Convert<T>(_childrenInfo, childIndex, requireValid: !_childrenConverter.DefaultValueIsInvalid);

    private static T?[] ConvertToArray<T>(ListConverter self, in DuckDbVectorInfo vector, int index)
    {
        var listRef = vector.UnsafeRead<DuckDbListRef>(index);
        var result = new T?[listRef.Length];
        for (int i = 0; i < listRef.Length; ++i)
            result[i] = self.ConvertChild<T>(listRef.Offset + i);
        return result;
    }

    private static ImmutableArray<T?> ConvertToImmutableArray<T>(
        ListConverter self, in DuckDbVectorInfo vector, int index) 
    {
        var listRef = vector.UnsafeRead<DuckDbListRef>(index);
        var builder = ImmutableArray.CreateBuilder<T?>(initialCapacity: listRef.Length);
        for (int i = 0; i < listRef.Length; ++i)
            builder.Add(self.ConvertChild<T>(listRef.Offset + i));
        return builder.MoveToImmutable();
    }
}
