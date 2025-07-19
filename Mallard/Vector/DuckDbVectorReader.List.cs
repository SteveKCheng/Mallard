using Mallard.Basics;
using Mallard.C_API;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Mallard;

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Retrieve the vector containing all the children across all lists in a vector of lists,
    /// allowing "raw" access (spans).
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind an element of the lists to.
    /// </typeparam>
    /// <param name="parent">
    /// The vector of lists.
    /// </param>
    /// <returns>
    /// The lists' children, collected into one vector, i.e. the "children vector" or "vector of list children".
    /// </returns>
    /// <exception cref="DuckDbException"></exception>
    public static DuckDbVectorRawReader<T> GetChildrenRawVector<T>(in this DuckDbVectorRawReader<DuckDbListRef> parent)
        where T : unmanaged, allows ref struct
        => new(parent._info.GetChildrenVectorInfo());

    /// <summary>
    /// Retrieve the vector containing all the children across all lists in a vector of lists.
    /// </summary>
    /// <typeparam name="T">
    /// The .NET type to bind an element of the lists to.
    /// </typeparam>
    /// <param name="parent">
    /// The vector of lists.
    /// </param>
    /// <returns>
    /// The lists' children, collected into one vector, i.e. the "children vector" or "vector of list children".
    /// </returns>
    /// <exception cref="DuckDbException"></exception>
    public static DuckDbVectorReader<T> GetChildrenVector<T>(in this DuckDbVectorRawReader<DuckDbListRef> parent)
        where T : notnull
        => new(parent._info.GetChildrenVectorInfo());

    internal unsafe static DuckDbVectorInfo GetChildrenVectorInfo(in this DuckDbVectorInfo parent)
    {
        var parentVector = parent.NativeVector;
        DuckDbVectorInfo.ThrowOnNullVector(parentVector);

        var childVector = NativeMethods.duckdb_list_vector_get_child(parentVector);
        if (childVector == null)
            throw new DuckDbException("Could not get the child vector from a list vector in DuckDB. ");

        var totalChildren = NativeMethods.duckdb_list_vector_get_size(parentVector);
        var childBasicType = DuckDbVectorInfo.GetVectorElementBasicType(childVector);

        return new DuckDbVectorInfo(childVector, childBasicType, (int)totalChildren);
    }
}

/// <summary>
/// Reports where the data for one list resides in a list-valued DuckDB vector.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDbListRef
{
    // We do not support vectors of length > int.MaxValue
    // (not sure if this is even possible in DuckDB itself).
    // But DuckDB's C API uses uint64_t which we must mimick here.
    // We unconditionally cast it to int in the properties below
    // so user code does not have to do so.
    private readonly ulong _offset;
    private readonly ulong _length;

    /// <summary>
    /// The index of the first item of the target list, within the list vector's
    /// "children vector".
    /// </summary>
    public int Offset => unchecked((int)_offset);

    /// <summary>
    /// The length of the target list.
    /// </summary>
    public int Length => unchecked((int)_length);
}

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

    private ListConverter(Type listType, in DuckDbVectorInfo parent)
    {
        _childrenInfo = DuckDbVectorMethods.GetChildrenVectorInfo(parent);
        _childrenConverter = VectorElementConverter.CreateForType(listType, _childrenInfo);
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForArrayImpl<T>(in DuckDbVectorInfo parent)
        => VectorElementConverter.Create(new ListConverter(typeof(T), parent), &ConvertToArray<T>);

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
    private unsafe static VectorElementConverter ConstructForImmutableArrayImpl<T>(in DuckDbVectorInfo parent)
        => VectorElementConverter.Create(new ListConverter(typeof(T), parent), &ConvertToImmutableArray<T>);

    public unsafe static VectorElementConverter ConstructForArray(Type listType, in DuckDbVectorInfo parent)
    {
        if (!listType.IsArray)
            throw new ArgumentException("The target type must be an array. ", nameof(listType));

        var childType = listType.GetElementType()!;
        var constructFunc = (delegate*<in DuckDbVectorInfo, VectorElementConverter>)
            ConstructForArrayImplMethod.MakeGenericMethod(childType).MethodHandle.GetFunctionPointer();
        return constructFunc(parent);
    }

    public unsafe static VectorElementConverter ConstructForImmutableArray(Type listType, in DuckDbVectorInfo parent)
    {
        if (!listType.IsInstanceOfGenericDefinition(typeof(ImmutableArray<>)))
            throw new ArgumentException("The target type must be ImmutableArray<T>. ", nameof(listType));

        var childType = listType.GetGenericArguments()[0];
        var constructFunc = (delegate*<in DuckDbVectorInfo, VectorElementConverter>)
            ConstructForImmutableArrayImplMethod.MakeGenericMethod(childType).MethodHandle.GetFunctionPointer();
        return constructFunc(parent);
    }

    private unsafe static T[] ConvertToArray<T>(ListConverter self, in DuckDbVectorInfo vector, int index)
    {
        var listRef = ((DuckDbListRef*)vector.DataPointer)[index];
        var result = new T[listRef.Length];
        for (int i = 0; i < listRef.Length; ++i)
            result[i] = self._childrenConverter.Invoke<T>(self._childrenInfo, listRef.Offset + i);
        return result;
    }

    private unsafe static ImmutableArray<T> ConvertToImmutableArray<T>(
        ListConverter self, in DuckDbVectorInfo vector, int index)
    {
        var listRef = ((DuckDbListRef*)vector.DataPointer)[index];
        var builder = ImmutableArray.CreateBuilder<T>(initialCapacity: listRef.Length);
        for (int i = 0; i < listRef.Length; ++i)
            builder.Add(self._childrenConverter.Invoke<T>(self._childrenInfo, listRef.Offset + i));
        return builder.MoveToImmutable();
    }
}