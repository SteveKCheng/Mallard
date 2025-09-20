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
        /// <summary>
        /// Reference count from the parent <see cref="HandleRefCount" />.
        /// Set to null if this instance is invalid.
        /// </summary>
        private ref int _counter;
        
        /// <summary>
        /// Whether this instance (scope) is the first of the active references.
        /// </summary>
        public bool IsFirst { get; }
        
        /// <summary>
        /// Whether this instance is valid.
        /// </summary>
        public bool IsValid => !Unsafe.IsNullRef(in _counter);

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
        /// Establishes the dynamic scope, on an owning object that is still alive.
        /// </summary>
        /// <param name="parent">The <see cref="HandleRefCount" /> instance that controls
        /// access on some owning managed object. </param>
        /// <remarks>
        /// If the owning object has already been disposed, establishing the dynamic
        /// scope fails, and is indicated by <see cref="IsValid" /> being false when this constructor
        /// returns.  No exception is thrown in that case.  
        /// </remarks>
        internal Scope(ref HandleRefCount parent)
        {
            ref int counter = ref parent._counter;

            int v = Interlocked.Increment(ref counter);
            if (v <= 0)
            {
                Interlocked.Decrement(ref counter);
                return;
            }

            _counter = ref counter;
            IsFirst = (v == 1);
        }

        /// <summary>
        /// Establishes the dynamic scope.
        /// </summary>
        /// <param name="parent">The <see cref="HandleRefCount" /> instance that controls
        /// access on some owning managed object. </param>
        /// <param name="resurrect">
        /// If true, this constructor will try to mark the owning object as resurrected,
        /// if it is dead (disposed).  If false, the behavior is the same as
        /// the other constructor, <see cref="Scope(ref HandleRefCount)" />. 
        /// </param>
        /// <remarks>
        /// If <paramref name="resurrect"/> is true, and
        /// the owning object has not already been disposed, establishing the dynamic
        /// scope fails, and is indicated by <see cref="IsValid" /> being false when this constructor
        /// returns.  No exception is thrown in that case.  
        /// </remarks>
        internal Scope(ref HandleRefCount parent, bool resurrect)
        {
            if (!resurrect)
            {
                this = new Scope(ref parent);
                return;
            }

            ref int counter = ref parent._counter;
            int v = counter;
            int w;
            do
            {
                // Fail if object is already alive
                if (v >= 0)
                    return;

                w = v;
                v = Interlocked.CompareExchange(ref counter, 1, w);
            } while (v != w);

            _counter = ref counter;
            IsFirst = true;
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
    /// <exception cref="ObjectDisposedException">
    /// The owning object of this instance has been indicated to be disposed 
    /// (or is in the middle of being disposed by another thread).
    /// </exception>
    /// <returns>Scope object that should be the subject of a <c>using</c> statement in C#. </returns>
    [UnscopedRef]
    public Scope EnterScope(object? targetObject)
    {
        var s = new Scope(ref this);
        if (!s.IsValid)
            throw new ObjectDisposedException(targetObject?.GetType().FullName, "Cannot operate on a disposed object. ");
        return s;
    }

    /// <summary>
    /// Transition from a disposed state to having one active reference.
    /// </summary>
    /// <param name="scope">
    /// Established dynamic scope with an active reference count of one if the object is
    /// considered successfully resurrected.  Otherwise the scope is invalid. 
    /// </param>
    /// <remarks>
    /// <para>
    /// Resurrecting objects requires special care by the caller.  Ordinarily,
    /// if resurrection is not allowed, and the owning object may establish the invariant
    /// that (some of) its member variables are immutable since construction of that
    /// object, and hence the variables may be thread-safe for reading as long as the
    /// object is alive (not disposed).  Then when <see cref="PrepareToDisposeOwner" />
    /// is called and returns true, the caller can perform clean up on those variables with
    /// the guarantee that no other thread will be attempting to read them again.
    /// </para>
    /// <para>
    /// When resurrection becomes allowed, the above thread-safety assumptions go out
    /// the window.  When <see cref="PrepareToDisposeOwner" /> is called,
    /// another thread can race to make the object alive again.  So member variables
    /// generally cannot be assumed to be immutable.  The loss of this assumption
    /// makes the code more complex and difficult to reason about, so making objects
    /// resurrectable is not recommended.  But in this library, some interfaces
    /// we have to implement for compatibility may imply resurrection of objects,
    /// and so this facility is made available.
    /// </para>
    /// </remarks>
    /// <returns>
    /// Whether the object has been successfully resurrected.
    /// </returns>
    [UnscopedRef]
    public bool TryResurrect(out Scope scope)
    {
        scope = new Scope(ref this, resurrect: true);
        return scope.IsValid;
    }
}
