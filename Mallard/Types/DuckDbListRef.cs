using System;
using System.Runtime.InteropServices;

namespace Mallard;

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

