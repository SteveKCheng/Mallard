namespace Mallard.Types;

/// <summary>
/// Dummy type used as the type parameter to <see cref="DuckDbVectorRawReader{T}" />
/// to read a vector of structs from DuckDB.
/// </summary>
/// <remarks>
/// This type holds no data itself, and it is uselss to instantiate it.
/// </remarks>
public readonly ref struct DuckDbStructRef
{
}
