using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.Basics;

internal static class ReflectionExtensions
{
    public static bool IsInstanceOfGenericDefinition(this Type targetType, Type genericType)
        => targetType.GetGenericTypeDefinition() == genericType;
}
