using System;

namespace Mallard;

/// <summary>
/// Selects conventions for mapping certain types from DuckDB to .NET.
/// </summary>
/// <remarks>
/// <para>
/// In Mallard's API, generally speaking, type mapping is driven by a explicitly-given
/// generic type parameter like in <see cref="DuckDbVectorReader{T}" />.  However,
/// ambiguities can arise in some situations:
/// 
/// <list type="bullet">
///   <item>
///     When the type parameter is <see cref="System.Object" /> (which indicates
///     the actual type should be casted/boxed)
///   </item>
///   <item>
///     When consuming the type via an object/interface implementing a non-Mallard API 
///     which assumes the type is known upfront without any explicit input
///     (such as ADO.NET's <see cref="System.Data.IDataRecord.GetFieldType(int)" />).
///   </item>
/// </list>
/// </para>
/// 
/// <para>
/// In these cases, Mallard must assume a "default" .NET type to map a DuckDB type
/// to.  These rules may depend on the context or application.  For instance,
/// under <see cref="DuckDbDataReader" />, columns of type DATE in DuckDB are
/// mapped to <see cref="System.DateTime" /> — for compatibility with existing ADO.NET-using
/// code, and other databases — even though <see cref="System.DateOnly" /> would be 
/// the better, modern choice.
/// </para>
/// <para>
/// This enumeration allows choosing conventions for various types when there is 
/// more than one reasonable choice.
/// </para>
/// <para>
/// Note that the conventions specified here do not apply when there is already
/// an explicit specification of the target type (like in the aforementioned
/// <see cref="DuckDbVectorReader{T}" />).
/// </para>
/// </remarks>
[Flags]
public enum DuckDbTypeMappingFlags
{
    /// <summary>
    /// Take the default conventions, which has none of the other flags in this enumeration
    /// set. 
    /// </summary>
    Default = 0,

    /// <summary>
    /// Map a DATE type coming from the database as <see cref="System.DateTime"/>, 
    /// for ADO.NET compatibility.
    /// </summary>
    /// <remarks>
    /// <para>
    /// If this flag is not present, a DATE type is mapped to <see cref="System.DateOnly" />.
    /// </para>
    /// </remarks>
    DatesAsDateTime = 0x1,
}
