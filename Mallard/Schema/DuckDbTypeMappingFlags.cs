using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard;

[Flags]
public enum DuckDbTypeMappingFlags
{
    Default = 0,

    /// <summary>
    /// Map a DATE type coming from the database as a DateTime, for ADO.NET compatibility.
    /// </summary>
    DatesAsDateTime = 0x1,
}
