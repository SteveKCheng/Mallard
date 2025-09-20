using Mallard.Interop;
using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Mallard;
using Mallard.Types;

/// <summary>
/// State object required to convert a DuckDB list and its contents to
/// a .NET array or array-like type.
/// </summary>
internal sealed class ListConverter
{
    private sealed class Binder : IConverterBinder<ListConverter>
    {
        private readonly DuckDbColumnInfo _childColumn;
        private readonly VectorElementConverter _childConverterUnbound;

        public unsafe ListConverter BindToVector(in DuckDbVectorInfo vector)
        {
            var childVector = vector.GetListChildrenVectorInfo(_childColumn);
            var childConverter = _childConverterUnbound.BindToVector(childVector);
            return new ListConverter(childVector, childConverter);
        }

        public unsafe Binder(Type? childType, ref readonly ConverterCreationContext parentContext)
        {
            using var parentLogicalType = new NativeLogicalTypeHolder(parentContext.GetNativeLogicalType());
            
            static _duckdb_logical_type* getChildLogicalType(_duckdb_logical_type* p)
                => NativeMethods.duckdb_list_type_child_type(p);

            using (var childLogicalType = new NativeLogicalTypeHolder(
                getChildLogicalType(parentLogicalType.NativeHandle)))
            {
                _childColumn = new DuckDbColumnInfo(childLogicalType.NativeHandle);
            }

            var childContext = ConverterCreationContext.Create(_childColumn,
                                                               parentLogicalType.NativeHandle,
                                                               &getChildLogicalType,
                                                               parentContext.TypeMapping,
                                                               parentContext.TypeMappingFlags);

            _childConverterUnbound = VectorElementConverter.CreateForType(childType, in childContext);

            if (!_childConverterUnbound.IsValid)
            {
                throw new ArgumentException(
                    childType != null
                        ? $"The element type of the list/array cannot be converted to .NET type {childType}. "
                        : "The element type of the list/array cannot be converted to any .NET type. ");
            }
        }

        public Type TargetType => _childConverterUnbound.TargetType;
    }

    /// <summary>
    /// Vector for the list children.
    /// </summary>
    private readonly DuckDbVectorInfo _childVector;

    /// <summary>
    /// Type converter for the list children.
    /// </summary>
    private readonly VectorElementConverter _childConverter;

    private static readonly MethodInfo ConstructForArrayImplMethod =
        typeof(ListConverter).GetMethod(nameof(ConstructForArrayImpl),
                                        BindingFlags.Static | BindingFlags.NonPublic)!;

    private static readonly MethodInfo ConstructForImmutableArrayImplMethod =
        typeof(ListConverter).GetMethod(nameof(ConstructForImmutableArrayImpl), 
                                        BindingFlags.Static | BindingFlags.NonPublic)!;

    private ListConverter(DuckDbVectorInfo childVector, VectorElementConverter childConverter)
    {
        _childVector = childVector;
        _childConverter = childConverter;
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForArrayImpl<T>(Binder binder, ref readonly ConverterCreationContext _)
        => VectorElementConverter.Create(binder, &ConvertToArray<T>);

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForImmutableArrayImpl<T>(Binder binder, ref readonly ConverterCreationContext _)
        => VectorElementConverter.Create(binder, &ConvertToImmutableArray<T>, defaultValueIsInvalid: true);

    /// <summary>
    /// Get the element converter for a .NET array of the specified type.
    /// </summary>
    /// <param name="childType">
    /// The element type of the array.  If null, it will be implied by the default rules in this library.
    /// </param>
    /// <param name="context">
    /// Refers to the list-valued DuckDB column to convert to a .NET array.
    /// </param>
    public static VectorElementConverter ConstructForArray(Type? childType, ref readonly ConverterCreationContext context)
    {
        var binder = new Binder(childType, in context);
        return VectorElementConverter.UnsafeCreateFromGeneric(ConstructForArrayImplMethod, 
                                                              binder, in context, binder.TargetType);
    }

    /// <summary>
    /// Get the element converter for an instantiation of <see cref="ImmutableArray{T}" />.
    /// </summary>
    /// <param name="childType">
    /// The element type of <see cref="ImmutableArray{T}" />.
    /// </param>
    /// <param name="context">
    /// Refers to the list-valued DuckDB column to convert to <see cref="ImmutableArray{T}" />.
    /// </param>
    public static VectorElementConverter ConstructForImmutableArray(Type childType, ref readonly ConverterCreationContext context)
    {
        var binder = new Binder(childType, in context);
        return VectorElementConverter.UnsafeCreateFromGeneric(ConstructForImmutableArrayImplMethod, 
                                                              binder, in context, childType);
    }

    private T? ConvertChild<T>(int childIndex)
        => _childConverter.Convert<T>(_childVector, childIndex, requireValid: !_childConverter.DefaultValueIsInvalid);

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
