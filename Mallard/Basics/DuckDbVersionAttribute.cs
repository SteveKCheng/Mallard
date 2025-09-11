using System;
using System.Reflection;

namespace Mallard;

/// <summary>
/// Reports the version of DuckDB that Mallard has been built against.
/// </summary>
/// <remarks>
/// This attribute is injected by MSBuild.
/// </remarks>
[AttributeUsage(AttributeTargets.Assembly,  AllowMultiple = false)]
internal sealed class DuckDbVersionAttribute(string value) : Attribute
{
    /// <summary>
    /// The version as a string.
    /// </summary>
    public string Value { get; } = value;

    /// <summary>
    /// Get the instance of this attribute for the containing assembly (for Mallard).
    /// </summary>
    public static DuckDbVersionAttribute Instance 
        => Assembly.GetExecutingAssembly().GetCustomAttribute<DuckDbVersionAttribute>()!; 
}
