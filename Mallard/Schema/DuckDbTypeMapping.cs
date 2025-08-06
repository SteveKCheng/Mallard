using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard;

/// <summary>
/// User-customizable settings that inform type conversion between DuckDB and .NET.
/// </summary>
/// <remarks>
/// </remarks>
public sealed class DuckDbTypeMapping
{
    /// <summary>
    /// Conventions for mapping certain types "by default".
    /// </summary>
    public DuckDbTypeMappingFlags TypeMappingFlags { get; }

    private DuckDbTypeMapping(DuckDbTypeMappingFlags typeMappingFlags)
    {
        TypeMappingFlags = typeMappingFlags;
    }

    /// <summary>
    /// The global instance of this class that informs type mapping in Mallard, 
    /// unless a different instance is passed in by the user.
    /// </summary>
    public static DuckDbTypeMapping Default { get; } = new DuckDbTypeMapping(DuckDbTypeMappingFlags.Default);
}
