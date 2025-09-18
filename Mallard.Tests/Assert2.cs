using System;
using Xunit.Sdk;
using Xunit;

namespace Mallard.Tests;

internal static class Assert2
{
    public static void Equal<T>(IEnumerable<T>? expected, IEnumerable<T>? actual) where T : IEquatable<T>
        => Equal(expected, actual, EqualityComparer<T>.Default); 

    // Replacement for Assert.Equal with the same argument types because
    // the implementation of that uses reflection that is incompatible with AOT
    public static void Equal<T>(IEnumerable<T>? expected, IEnumerable<T>? actual, IEqualityComparer<T> comparer)
    {
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }

        Assert.NotNull(actual);

        using IEnumerator<T> enumeratorExpected = expected.GetEnumerator();
        using IEnumerator<T> enumeratorActual = actual.GetEnumerator();
        int count = 0;
        while (true)
        {
            var hasExpected = enumeratorExpected.MoveNext();
            var hasActual = enumeratorActual.MoveNext();
            
            if (!hasExpected && !hasActual)
                break;
            
            Assert.True(hasExpected && hasActual,
                $"Number of elements in expected and actual sequence differ: one has only {count} elements and the other has more");
            
            var expectedCurrent = enumeratorExpected.Current;
            var actualCurrent = enumeratorActual.Current;

            bool compareSuccessful = false;
            Exception? savedException = null;
            try
            {
                compareSuccessful = comparer.Equals(expectedCurrent, actualCurrent);
            }
            catch (Exception e)
            {
                savedException = e;
            }

            if (!compareSuccessful)
            {
                var expectedString = expectedCurrent?.ToString() ?? "(null)";
                var expectedType = expectedCurrent?.GetType().ToString() ?? "(null)";
                var actualString = actualCurrent?.ToString() ?? "(null)";
                var actualType = actualCurrent?.GetType().ToString() ?? "(null)";
                
                throw EqualException.ForMismatchedCollectionsWithError(
                    count,
                    expectedString, null, expectedType,
                    actualString, null, actualType, savedException);
            }

            ++count;
        }
    }
}