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

    private unsafe ListConverter(Type? childType, ref readonly ConverterCreationContext context)
    {
        _childrenInfo = DuckDbVectorMethods.GetListChildrenVectorInfo(context.NativeVector);
        var childContext = new ConverterCreationContext(_childrenInfo.ColumnInfo, _childrenInfo.NativeVector);
        _childrenConverter = VectorElementConverter.CreateForType(childType, in childContext);
        if (!_childrenConverter.IsValid)
        {
            throw new ArgumentException(
                childType != null 
                    ? $"The element type of the list/array cannot be converted to .NET type {childType}. "
                    : "The element type of the list/array cannot be converted to any .NET type. ");
        }
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForArrayImpl<T>(ListConverter self, in DuckDbVectorInfo _)
        => VectorElementConverter.Create(self, &ConvertToArray<T>);

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForImmutableArrayImpl<T>(ListConverter self, in DuckDbVectorInfo _)
        => VectorElementConverter.Create(self, &ConvertToImmutableArray<T>, defaultValueIsInvalid: true);

    /// <summary>
    /// Get the element converter for a .NET array of the specified type.
    /// </summary>
    /// <param name="elementType">
    /// The element type of the array.  If null, it will be implied from <paramref name="vector" />
    /// via the default rules.
    /// </param>
    /// <param name="context">
    /// Refers to the list-valued DuckDB column to convert to a .NET array.
    /// </param>
    public static VectorElementConverter ConstructForArray(Type? childType, ref readonly ConverterCreationContext context)
    {
        var self = new ListConverter(childType, in context);
        childType = self._childrenConverter.TargetType;

        return VectorElementConverter.UnsafeCreateFromGeneric(ConstructForArrayImplMethod, 
                                                              self, in context, childType);
    }

    /// <summary>
    /// Get the element converter for an instantiation of <see cref="ImmutableArray{T}" />.
    /// </summary>
    /// <param name="elementType">
    /// The element type of <see cref="ImmutableArray{T}" />.
    /// </param>
    /// <param name="context">
    /// Refers to the list-valued DuckDB column to convert to <see cref="ImmutableArray{T}" />.
    /// </param>
    public static VectorElementConverter ConstructForImmutableArray(Type elementType, ref readonly ConverterCreationContext context)
    {
        var self = new ListConverter(elementType, in context);

        return VectorElementConverter.UnsafeCreateFromGeneric(ConstructForImmutableArrayImplMethod, 
                                                              self, in context, elementType);
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
