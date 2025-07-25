using System;
using System.Runtime.CompilerServices;

namespace Mallard;

internal static class ReflectionExtensions
{
    /// <summary>
    /// Extract the type parameter from an instance of a generic type,
    /// if the generic type matches the one given.
    /// </summary>
    /// <param name="targetType">
    /// The (closed) type to match against.
    /// </param>
    /// <param name="genericType">
    /// Open generic type of one type parameter.
    /// </param>
    /// <returns>
    /// The sole type parameter if <paramref name="targetType" /> matches
    /// <paramref name="genericType" />, or null if it does not match.
    /// </returns>
    public static Type? GetGenericUnderlyingType(this Type targetType, Type genericType)
    {
        if (targetType.GetGenericTypeDefinition() == genericType)
        {
            var paramTypes = targetType.GetGenericArguments();
            if (paramTypes.Length == 1)
                return paramTypes[0];
        }

        return null;
    }

    /// <summary>
    /// Determine whether a type is a nullable value type.
    /// </summary>
    /// <param name="type">The type to examine.  </param>
    /// <returns>True if <paramref name="type"/> is a closed (value)
    /// type of <see cref="Nullable{T}"/>, false otherwise. </returns>
    /// <remarks>
    /// <para>
    /// The .NET JIT specifically recognizes this pattern when 
    /// <paramref name="type" /> is <c>typeof(T)</c> for a generic
    /// parameter <c>T</c>, and optimizes the evaluation to a constant.
    /// (Check <a href="https://sharplab.io/#v2:EYLgxg9gTgpgtADwGwBYA0AXEBDAzgWwB8ABAJgEYBYAKGIAYACY8lAbhvoYGUALbKAA4AZbMAB0AJQCuAOwwBLfDHbUOAZibkkTUgwDCDGgG8aDMwwDaAKXkYA4jBkwo8sAAoMATwEwIAMzd5OQBKYIBdU3NrWwcnF3cvH39AuQB+UIjqcyYNZm1iFAYAFRhcDAAeIoA+N2CGEyzs82AICAAbBmAGAF4GAElcADkpNrbRNpgPb18AotCVJvMABRc5CVKRjDdgYIXzAF8aSLNiXK0mQpKyrgxVgHNanqri0ormOhrd45zNfMvXoTyMqPbrPK4VQFlcpBDBVT57MzfU6/C4MFYw9a4TbbVodAR1b4NRYncgATjcAgYqQYACIBgwZCMxsAJjSGCBafSZBAMAymeMYDSvo0zIcRYZxci8p1cf0hvyWZMitMGBhgt9sqDVWIBgA1bBtKQwZU+BgAMjNGsWGB1uFizlcJpg5rNEuJ2RtDnsjgdYCdABEYH4grZ5BAZCDeokZm5hqMBeUqsL9kA===">sharplab.io</a>.)
    /// </para>
    /// <para>
    /// On the other hand, the JIT cannot optimize a check of
    /// <see cref="Nullable.GetUnderlyingType(Type)" /> being non-null,
    /// nearly as well.  See also the discussion under
    /// <a href="https://github.com/dotnet/runtime/issues/45177">.NET runtime GitHub PR #45177</a>.
    /// </para>
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNullable(this Type type)
        => type.IsValueType &&
           type.IsGenericType && 
           type.GetGenericTypeDefinition() == typeof(Nullable<>);
}
