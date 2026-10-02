using System;
using System.Runtime.InteropServices;

namespace Mallard.Types;

/// <summary>
/// Reports where the data for one list resides in a list-valued DuckDB vector.
/// </summary>
/// <remarks>
/// <para>
/// This structure does not contain references itself but, refers to the
/// list members by offset, which exist elsewhere.  It cannot be written directly
/// to DuckDB vectors since DuckDB's cooperation is needed to allocate memory
/// for the list members.
/// </para>
/// <para>
/// To prevent such attempts in the first place, this type is made into a "ref struct",
/// and so cannot be substituted into the type parameter of
/// <see cref="DuckDbVectorMethods.AsSpan{T}(in DuckDbVectorRawWriter{T})" />.
/// This restriction also disallows reading this type through a span from
/// <see cref="DuckDbVectorMethods.AsSpan{T}(in DuckDbVectorRawReader{T})" />,
/// which is arguably always safe, but is not really useful functionality. 
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly ref struct DuckDbListRef
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
