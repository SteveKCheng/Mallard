using System;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// Common methods on the various types of "readers" for DuckDB vectors,
/// for the benefit of generic code.
/// </summary>
/// <remarks>
/// This interface does not allow access to the element values as that
/// obviously depends on their type.  Use <see cref="IDuckDbVector{T}" />
/// for that functionality.
/// </remarks>
public interface IDuckDbVector
{
    /// <summary>
    /// Return whether an element of this vector is valid (not null).
    /// </summary>
    /// <param name="index">
    /// The index of the element of the vector.
    /// </param>
    /// <returns>
    /// True if valid (non-null), false if invalid (null).
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    bool IsItemValid(int index);

    /// <summary>
    /// The variable-length bit mask indicating which elements in the vector are valid (not null).
    /// </summary>
    /// <remarks>
    /// For element index <c>i</c> and validity mask <c>m</c> (the return value from this method), 
    /// the following expression indicates if the element is valid:
    /// <code>
    /// m.Length == 0 || (m[i / 64] &amp; (1u % 64)) != 0
    /// </code>
    /// </remarks>
    ReadOnlySpan<ulong> ValidityMask { get; }

    /// <summary>
    /// The length (number of rows) inherited from the result chunk this vector is part of.
    /// </summary>
    int Length { get; }

    /// <summary>
    /// Information about the column that this vector is part of.
    /// </summary>
    DuckDbColumnInfo ColumnInfo { get; }
}

/// <summary>
/// Methods to retrieve items from DuckDB vectors.
/// </summary>
/// <remarks>
/// This interface differs from <see cref="System.Collections.Generic.IReadOnlyList{T}" />
/// in two ways: firstly, there is dedicated handling of null values; and secondly,
/// DuckDB vector readers are "ref structs" so they cannot implement enumerators without
/// copying and converting element values to GC memory, which is obviously inefficient.
/// </remarks>
public interface IDuckDbVector<T> : IDuckDbVector where T: allows ref struct
{
    /// <summary>
    /// Retrieve an element of this vector, returning the default value if it is invalid.
    /// </summary>
    /// <param name="index">The index of the element to select. </param>
    /// <returns>The desired element of this vector. </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    /// <remarks>
    /// The "default" value refers to the default-initialized value of a variable of
    /// tyoe <typeparamref name="T" />.
    /// </remarks>
    T? GetItemOrDefault(int index);

    /// <summary>
    /// Retrieve a valid element of this vector.
    /// </summary>
    /// <param name="index">The index of the element to select. </param>
    /// <returns>The desired element of this vector. </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    /// <exception cref="InvalidOperationException">The requested element is invalid. </exception>
    /// <remarks>
    /// <para>
    /// For the purposes of this method, an element that is missing in the DuckDB vector
    /// but is materialized as an instance of <see cref="Nullable{U}" /> is considered
    /// "valid", i.e. this method will not throw an exception.  
    /// </para>
    /// <para>
    /// (Otherwise, there would be no point
    /// in substituting a nullable value type for <typeparamref name="T" />.  Consistency would
    /// argue that nullable reference types be treated the same way, but in .NET, reference 
    /// nullability are only compiler-driven annotations and not part of the run-time type
    /// system, so an implementation of this interface generally cannot detect whether
    /// <typeparamref name="T" /> is <c>U</c> or <c>U?</c> for reference types <c>U</c>.
    /// So, if <typeparamref name="T" /> is a reference type, this method throws an exception
    /// if the source element in the DuckDB vector is missing.)
    /// </para>
    /// <para>
    /// Clients that want to basically assume all the elements of the vector are valid
    /// would call this method.
    /// </para>
    /// </remarks>
    T GetItem(int index);

    /// <summary>
    /// Retrieve an element of this vector, or report that it is invalid.
    /// </summary>
    /// <param name="index">The index of the element in this vector. </param>
    /// <param name="item">The item that is to be read out.  Set to the
    /// element type's default value when the element is invalid.
    /// </param>
    /// <returns>
    /// Whether the element is valid.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">The index is out of range for the vector. </exception>
    bool TryGetItem(int index, [MaybeNullWhen(returnValue: false)] out T item);
}
