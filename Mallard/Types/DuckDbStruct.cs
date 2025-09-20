using System;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace Mallard.Types;

/// <summary>
/// .NET representation of an instance of a structure type from DuckDB, where the member values are boxed.
/// </summary>
/// <remarks>
/// <para>
/// This representation is inefficient because of the boxing, but avoids generics (of arbitrary arity).
/// </para>
/// <para>
/// Instances of this type are intended to be immutable (once created), but if the type of a member
/// is a reference type, or a mutable value type (i.e. not <c>readonly</c> in C#), user code may
/// be able to mutate the member value through the object reference that is made available
/// (even in safe code).  It is, obviously, highly discouraged to do so, but no direct memory 
/// corruption will occur should that be attempted.
/// </para>
/// </remarks>
public readonly struct DuckDbStruct : ITuple
{
    private readonly ImmutableArray<object?> _memberValues;

    internal DuckDbStruct(ImmutableArray<object?> memberValues)
    {
        _memberValues = memberValues;
    }

    /// <summary>
    /// Get a member of the structure by its index.
    /// </summary>
    /// <param name="index">
    /// The index of the member, in the range from 0 (inclusive) to <see cref="Length" /> (exclusive).
    /// </param>
    /// <returns>
    /// The value of the member, boxed/casted to <see cref="object" />.
    /// </returns>
    public object? this[int index] => _memberValues[index];

    /// <summary>
    /// The number of members in the structure.
    /// </summary>
    public int Length => _memberValues.Length;

    /// <summary>
    /// Get the values of the members of the structure as an immutable array of boxed objects.
    /// </summary>
    public ImmutableArray<object?> ToImmutableArray() => _memberValues;
}
