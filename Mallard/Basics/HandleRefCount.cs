using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Mallard;

/// <summary>
/// Internal helper to implement "safe handle" functionality on objects
/// that maintain ownership of resources (from a native library) which
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
/// made available (for the object protected by this helper), other than its destruction 
/// is thread-safe.  If not, the solution
/// is not reference counting, but simply a lock or barricade to protect all operations.
/// </para>
/// <para>
/// Because the "lock scope" holds a (managed) reference to the counter, that it needs
/// for "unlocking" (disposal), it prevents the object that holds the counter from being 
/// garbage-collected while the scope is active.  This fact obviates the need to sprinke
/// <see cref="GC.KeepAlive(object?)" /> on <c>this</c> when using pointers to 
/// native objects stored inside the <c>this</c> managed object.
/// </para>
/// <para>
/// Re-entrancy is allowed.  
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

    /// <summary>
    /// Dynamic scope where shared ownership of some resource is to be taken by the current
    /// thread, within an object that uses <see cref="HandleRefCount" /> to control
    /// multi-threaded access.
    /// </summary>
    public ref struct Scope
    {
        private ref int _counter;
        
        /// <summary>
        /// Whether this instance (scope) is the first of the active references.
        /// </summary>
        public bool IsFirst { get; }

        /// <summary>
        /// Throw an exception if this scope is not the first of active references.
        /// Used to enforce mutual exclusion.
        /// </summary>
        public void ThrowIfNotFirst()
        {
            if (!IsFirst)
            {
                throw new InvalidOperationException(
                    "This method is not allowed to be called simultaneously from multiple threads. ");
            }
        }

        /// <summary>
        /// Establishes the dynamic scope.
        /// </summary>
        /// <param name="parent">The <see cref="HandleRefCount" /> instance that controls
        /// access on some owning managed object. </param>
        /// <param name="targetObject">
        /// The managed object, used only for reporting errors when the dynamic scope cannot be entered.
        /// </param>
        /// <exception cref="ObjectDisposedException">
        /// <paramref name="parent" /> indicates its owning object has already been disposed 
        /// (or is in the middle of being disposed by another thread).
        /// </exception>
        internal Scope(ref HandleRefCount parent, object targetObject)
        {
            _counter = ref parent._counter;

            int v = Interlocked.Increment(ref _counter);
            if (v <= 0)
            {
                Interlocked.Decrement(ref _counter);
                throw new ObjectDisposedException(targetObject.GetType().FullName, "Cannot operate on a disposed object. ");
            }

            IsFirst = (v == 1);
        }

        /// <summary>
        /// Exit the dynamic scope.
        /// </summary>
        public void Dispose()
        {
            Interlocked.Decrement(ref _counter);

            // Defend against calling Dispose multiple times
            _counter = ref Unsafe.NullRef<int>();   
        }
    }

    /// <summary>
    /// Prepare to destroy the resources where this <see cref="HandleRefCount" /> is used 
    /// to control multi-threaded access.
    /// </summary>
    /// <returns>
    /// True if the object is now ready for disposal.  False if it is already disposed.
    /// </returns>
    /// <remarks>
    /// The flag for disposal is atomically flipped, so at most one thread will see a true value.
    /// That means a disposal that is guarded by the return value will not race between different threads.
    /// </remarks>
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

            // Using int.MinValue allows using Interlocked.Increment to take a reference count
            // in Scope's constructor, instead of a manual compare-exchange loop.  If -1 is used
            // as the sentinel, a second thread entering that constructor could see a temporary
            // value of 0 indicating a valid object when the object is actually disposed.
        } while ((v = Interlocked.CompareExchange(ref _counter, int.MinValue, w)) != w);

        return true;
    }

    /// <summary>
    /// Block <see cref="PrepareToDisposeOwner" /> from ever succeeding, by adding one permanently to
    /// the active count of references.
    /// </summary>
    /// <exception cref="ObjectDisposedException">
    /// The controlled object is already disposed.
    /// </exception>
    public void PreventDispose(object targetObject)
    {
        int v = Interlocked.Increment(ref _counter);
        if (v <= 0)
        {
            Interlocked.Decrement(ref _counter);
            throw new ObjectDisposedException(targetObject.GetType().FullName, "The object is already disposed. ");
        }
    }

    /// <summary>
    /// Enter a dynamic scope where shared ownership of an object is to be taken.
    /// </summary>
    /// <param name="targetObject">
    /// The managed object, used only for reporting errors when the dynamic scope cannot be entered.
    /// </param>
    /// <returns>Scope object that should be the subject of a <c>using</c> statement in C#. </returns>
    [UnscopedRef]
    public Scope EnterScope(object targetObject)
        => new Scope(ref this, targetObject);
}
