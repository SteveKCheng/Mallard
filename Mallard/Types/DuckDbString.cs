using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text;

namespace Mallard;

/// <summary>
/// Represents a string in DuckDB, as an element in some vector.
/// </summary>
/// <remarks>
/// <para>
/// This type is only used for reading elements from a DuckDB vector.  
/// (In DuckDB's C API, it corresponds to <c>duckdb_string_t</c>.)
/// It is a "ref struct"
/// as it may internally point to natively-allocated memory, which must be scoped to the
/// lifetime of the vector (<see cref="DuckDbVectorReader{T}" />).
/// </para>
/// <para>Semantically, this structure
/// is nothing more than <c>ReadOnlySpan&lt;byte&gt;</c> on the UTF-8 data
/// which can be accessed through the property <see cref="Utf8" />.
/// </para>
/// <para>
/// This type is not used for sending values from .NET to DuckDB, since DuckDB needs to allocate
/// and manage the memory blocks used to hold variable-length data, and such operations cannot
/// be surfaced safely using .NET structures alone.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly ref struct DuckDbString : IStatelesslyConvertible<DuckDbString, string>
{
    internal readonly DuckDbBlob _blob;

    /// <summary>
    /// Convert the UTF-8 string from DuckDB into a .NET string (in UTF-16).
    /// </summary>
    /// <returns>The string in UTF-16 encoding. </returns>
    public override string ToString() => Encoding.UTF8.GetString(_blob.Span);

    static string IStatelesslyConvertible<DuckDbString, string>.Convert(ref readonly DuckDbString item)
        => item.ToString();

    /// <summary>
    /// The UTF-8 bytes of the string.
    /// </summary>
    /// <remarks>
    /// <para>
    /// There is an inlined buffer within <see cref="DuckDbString" /> for short strings which the 
    /// returned span may point to.
    /// So this instance must outlive the span, enforced by this property being marked to be
    /// an "unscoped reference".
    /// </para>
    /// </remarks>
    [UnscopedRef]
    public ReadOnlySpan<byte> Utf8 => _blob.Span;
}
