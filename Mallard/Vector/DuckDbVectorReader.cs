using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// Points to data for a column within a result chunk from DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// DuckDB, a column-oriented database, calls this grouping of data a "vector".  
/// This type only supports reading from a DuckDB vector; writing to a vector
/// (for the purposes of modifying the database) requires a different shape of API
/// to enforce safety.
/// </para>
/// <para>
/// This reader internally uses indirect calls to read, and convert if necessary,
/// the data in DuckDB's native formats, to the desired .NET type.  It is thus
/// slower than <see cref="DuckDbVectorRawReader{T}" /> but the results are easier
/// for clients to consume.
/// </para>
/// </remarks>
public unsafe readonly ref struct 
    DuckDbVectorReader<T> 
    : IDuckDbVector<T>
    where T : notnull
{
    /// <summary>
    /// Type information and native pointers on this DuckDB vector.
    /// </summary>
    internal readonly DuckDbVectorInfo _info;

    /// <summary>
    /// Opaque (cached) state to pass to <see cref="_converterFunc" />.
    /// </summary>
    private readonly object? _converterState;

    /// <summary>
    /// Pointer to function that reads an element of the vector and converts it to 
    /// type <typeparamref name="T" />.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A function pointer is used instead of a virtual method to avoid
    /// allocating objects in simple cases (not involving nested/composite types).
    /// </para>
    /// <para>
    /// While for built-in types, an indirect call is not necessary (and in fact 
    /// <see cref="DuckDbVectorRawReader{T}" /> uses direct calls), 
    /// it is necessary when dealing with nested
    /// types (necessarily with non-null <see cref="_converterState" />).
    /// Of course, the flexibility of indirect call allows us to add user-customizable
    /// conversions later on.
    /// </para>
    /// <para>
    /// The arguments to the function are as follows:
    /// </para>
    /// <list type="table">
    ///   <listheader>
    ///     <term>Parameter</term>  
    ///     <description>Description</description>
    ///   </listheader>
    ///   <item>
    ///     <term><c>object?</c> <c>state</c></term>
    ///     <description>Cached state used by the conversion function. </description>
    ///   </item>
    ///   <item>
    ///     <term><c>DuckDbVectorInfo*</c> <c>vector</c></term>
    ///     <description>Information on the vector.  This argument is passed by
    ///     pointer rather than through <c>in</c> (read-only reference) 
    ///     only to work around a limitation of C#
    ///     where reference arguments in function pointers cannot be marked <c>scoped</c>;
    ///     without restricting the scope the code will not compile.
    ///   </item>
    ///   <item>
    ///     <term><c>int</c> <c>index</c></term>
    ///     <description>The index of the desired element of the vector. 
    ///     The callee may assume the index refers to a valid element. 
    ///     The caller is responsible for checking the index is valid
    ///     and refers to a valid (non-null) element.
    ///     </description>
    ///   </item>
    /// </list>
    /// </remarks>
    private readonly delegate*<object?, DuckDbVectorInfo*, int, T> _converterFunc;

    internal DuckDbVectorReader(scoped in DuckDbVectorInfo info)
    {
        _info = info;
        _converterState = null;
        _converterFunc = GetConverterFunction(_info.StorageType);

        if (_converterFunc == null)
            DuckDbVectorInfo.ThrowForWrongParamType(info.BasicType, info.StorageType, typeof(T));
    }

    private static delegate*<object?, DuckDbVectorInfo*, int, T> 
        GetConverterFunction(DuckDbBasicType storageType)
    {
        if (DuckDbVectorInfo.ValidateElementType<T>(storageType))
            return &PrimitiveRead;

        if (typeof(T) == typeof(string) && storageType == DuckDbBasicType.VarChar)
            return CastConversionFunc(&DuckDbString.ReadStringFromVector);

        return null;
    }

    /// <summary>
    /// Read a "primitive" element, i.e. one whose memory representation in DuckDB is exactly
    /// the same as the .NET type <typeparamref name="T"/>.
    /// </summary>
    /// <remarks>
    /// This function is for setting into <see cref="_converterFunc" />.
    /// </remarks>
    private static T PrimitiveRead(object? state, DuckDbVectorInfo* vector, int index)
    {
        // "This takes the address of, gets the size of, or declares a pointer to a managed type"
        // We never call this method for T being a managed type. 
        // Unfortunately we cannot express that constraint in C#, without exhaustively
        // listing the unmanaged types we allow here (which is possible but tedious).
#pragma warning disable CS8500 
        var p = (T*)vector->DataPointer;
#pragma warning restore CS8500
        return p[index];
    }

    /// <summary>
    /// Cast a function pointer for <see cref="_converterFunc" /> when
    /// <typeparamref name="U"/> is dynamically equal to <typeparamref name="T" />
    /// but is not known to be so statically.
    /// </summary>
    /// <remarks>
    /// This is a work around for a limitation in .NET's generics.
    /// When trying to "specialize" the code in this class on a specific type 
    /// <typeparamref name="U" /> (e.g. <c>string</c>) conditional on type tests, 
    /// the C# compiler and the .NET system deduce that the code that falls under
    /// the condition has <typeparamref name="T" /> equal to <typeparamref name="U" />,
    /// and therefore will not allow an assignment of the function pointer for
    /// conversion of type <typeparamref name="U" /> into a function pointer for
    /// type <typeparamref name="T" />.  We use this method to cast function pointers
    /// so the code can compile.  Naturally, <typeparamref name="U" /> must be
    /// dynamically equal to <typeparamref name="T" /> or else the .NET runtime
    /// system would be corrupted.
    /// </remarks>
    private static delegate*<object?, DuckDbVectorInfo*, int, T> 
        CastConversionFunc<U>(delegate*<object?, DuckDbVectorInfo*, int, U> p)
        => (delegate*<object?, DuckDbVectorInfo*, int, T>)p;

    /// <inheritdoc cref="IDuckDbVector.ValidityMask" />
    public ReadOnlySpan<ulong> ValidityMask => _info.ValidityMask;

    /// <inheritdoc cref="IDuckDbVector.IsItemValid(int)" />
    public bool IsItemValid(int index) => _info.IsItemValid(index);

    /// <inheritdoc cref="IDuckDbVector.Length" />
    public int Length => _info.Length;

    /// <inheritdoc cref="IDuckDbVector{T}.GetItem(int)" />
    public T GetItem(int index)
    {
        if (!TryGetItem(index, out var item))
            DuckDbVectorInfo.ThrowForInvalidElement(index);

        return item;
    }

    /// <inheritdoc cref="IDuckDbVector{T}.TryGetItem(int, out T)" />
    public bool TryGetItem(int index, [MaybeNullWhen(returnValue: false)] out T item)
    {
        if (_info.IsItemValid(index))
        {
            // A ref struct cannot be in GC memory, but nevertheless C# requires
            // its member to be "fixed" (even though it must be a no-op).
            fixed (DuckDbVectorInfo* infoPtr = &_info)
                item = _converterFunc(_converterState, infoPtr, index);
            return true;
        }
        else
        {
            item = default;
            return false;
        }
    }
}
