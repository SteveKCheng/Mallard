using System;

namespace Mallard;

internal static class ReflectionExtensions
{
    public static bool IsInstanceOfGenericDefinition(this Type targetType, Type genericType)
        => targetType.GetGenericTypeDefinition() == genericType;   
}
