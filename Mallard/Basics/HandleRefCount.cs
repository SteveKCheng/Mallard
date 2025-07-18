using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Mallard;

internal static class RefCountMethods
{
    public static HandleRefCount.Scope EnterScope(this ref HandleRefCount handleRefCount, object targetObject)
        => new(ref handleRefCount, targetObject);
}

/// <summary>
/// Internal helper to implement "safe handle" functionality on objects
/// that maintain ownership of objects from a native library which
/// must be explicitly destroyed.
/// </summary>
/// <remarks>
/// <para>
/// Helps to ensure an object cannot be disposed while it is being used by
/// another thread.  It is basically what <see cref="System.Runtime.InteropServices.SafeHandle" />
/// does, but more efficient and with a nicer syntax, in the author's opinion.  
/// In particular, the handle can be typed as pointers to unmanaged structs
/// rather than being type-erased to <see cref="IntPtr" />.  DuckDB's C API uses
/// double pointers fairly frequently and so it is easy to mistakenly pass a pointer with
/// the wrong level of indirection if everything is just <see cref="IntPtr" />.
/// </para>
/// <para>
/// This mechanism implicitly assumes every operation in the native library
/// for the object other than its destruction is thread-safe.  If not, the solution
/// is not reference counting, but simply a lock to protect all operations.
/// </para>
/// </remarks>
internal struct HandleRefCount
{
    /// <summary>
    /// Counter of number of active references.
    /// </summary>
    /// <remarks>
    /// This counter shall be updated atomically.  A value of zero means there are no
    /// active references, and the object may be disposed.  A negative value means
    /// the object has already been disposed or is being disposed concurrently.
    /// </remarks>
    private int _counter;

    public ref struct Scope
    {
        private ref int _counter;

        public Scope(ref HandleRefCount parent, object targetObject)
        {
            _counter = ref parent._counter;

            int v = Interlocked.Increment(ref _counter);
            if (v <= 0)
            {
                Interlocked.Decrement(ref _counter);
                throw new ObjectDisposedException(targetObject.GetType().FullName, "Cannot operate on a disposed object. ");
            }
        }

        public void Dispose()
        {
            Interlocked.Decrement(ref _counter);

            // Defend against calling Dispose multiple times
            _counter = ref Unsafe.NullRef<int>();   
        }
    }

    public bool PrepareToDisposeOwner()
    {
        int v = _counter;
        int w;
        do
        {
            // Already disposed.
            if (v < 0)
                return false;

            if (v > 0)
                throw new InvalidOperationException("Cannot dispose object while it is being used from another thread. ");

            w = v;
        } while ((v = Interlocked.CompareExchange(ref _counter, int.MinValue, w)) != w);

        return true;
    }
}
