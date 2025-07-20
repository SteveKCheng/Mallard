using System;
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
/// is nothing more than <see cref="ReadOnlySpan{byte}" /> on the UTF-8 data,
/// which can be accessed through the extension method 
/// <see cref="DuckDbVectorMethods.AsUtf8(ref readonly Mallard.DuckDbString)" />.
/// </para>
/// <para>
/// This type is not used for sending values from .NET to DuckDB, since DuckDB needs to allocate
/// and manage the memory blocks used to hold variable-length data, and such operations cannot
/// be surfaced safely using .NET structures alone.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly ref struct DuckDbString
{
    internal readonly DuckDbBlob _blob;

    /// <summary>
    /// Implementation of reading an element for <see cref="DuckDbVectorReader{string}" />.
    /// </summary>
    private static string ReadStringFromVector(object? state, in DuckDbVectorInfo vector, int index)
        => vector.UnsafeRead<DuckDbString>(index).ToString();

    internal unsafe static VectorElementConverter VectorElementConverter 
        => VectorElementConverter.Create(&ReadStringFromVector);

    /// <summary>
    /// Convert the UTF-8 string from DuckDB into a .NET string (in UTF-16).
    /// </summary>
    /// <returns>The string in UTF-16 encoding. </returns>
    public override string ToString() => Encoding.UTF8.GetString(DuckDbBlob.AsSpan(in _blob));
}

public static partial class DuckDbVectorMethods
{
    /// <summary>
    /// Get the UTF-8 bytes of the string.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This functionality exists as an extension method rather than an instance method 
    /// of <see cref="DuckDbBlob" /> only so that the "ref readonly" qualifier can be
    /// applied to the receiver argument in C#.
    /// </para>
    /// </remarks>
    /// <param name="nativeString">
    /// The native DuckDB structure representing the string.
    /// The "ref readonly" qualifier is only to ensure that the argument is an lvalue,
    /// so that the returned span does not point to a rvalue that might disappear
    /// (go out of scope) before the span does.  There is an inlined string buffer
    /// within <see cref="DuckDbString" /> for short strings which the span may point to.
    /// </param>
    public static ReadOnlySpan<byte> AsUtf8(ref readonly this DuckDbString nativeString)
        => DuckDbBlob.AsSpan(in nativeString._blob);
}
