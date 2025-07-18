using System;

namespace Mallard;

/// <summary>
/// Common methods on the various types of "readers" for DuckDB vectors,
/// for the benefit of generic code.
/// </summary>
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
}
