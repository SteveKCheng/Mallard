namespace Mallard.Types;

/// <summary>
/// Dummy element type for <see cref="DuckDbVectorRawReader{T}" /> to consume
/// fixed-length arrays from a DuckDB vector.
/// </summary>
/// <remarks>
/// <para>
/// This type holds no data itself, and it is useless to instantiate it.
/// </para>
/// <para>
/// Accessing structure members from DuckDB vectors always require memory-pointer indirection,
/// so this type is defined as a "ref struct"  to signal those semantics to the .NET type
/// system.
/// </para>
/// <para>
/// In particular, the "ref struct" constraint prevents forming spans of this type
/// from <see cref="DuckDbVectorMethods" /> (at compile-time).  DuckDB struct values
/// are arranged "column-wise", and plain spans of this type cannot be used for
/// direct access to the DuckDB vector. 
/// </para>
/// </remarks>
public readonly ref struct DuckDbArrayRef
{
}
