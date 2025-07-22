using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Mallard;

internal static partial class SynchronizationMethods
{
    /// <summary>
    /// Enter a dynamic scope where unique ownership of an object is to be taken.
    /// </summary>
    /// <param name="parent">The <see cref="HandleRefCount" /> instance that controls
    /// access on some owning managed object. </param>
    /// <param name="targetObject">
    /// The managed object, used only for reporting errors when the dynamic scope cannot be entered.
    /// </param>
    /// <returns>Scope object that should be the subject of a <c>using</c> statement in C#. </returns>
    public static Barricade.Scope EnterScope(this ref Barricade barricade, object targetObject)
        => new(ref barricade, targetObject);
}

/// <summary>
/// Internal helper to implement a "lock" on objects
/// that maintain ownership of resources (from a native library) which
/// must be explicitly destroyed.
/// </summary>
/// <remarks>
/// <para>
/// A "barricade" is a form of mutual exclusion or critical section.
/// </para>
/// <para>
/// This type is similar to <see cref="HandleRefCount" /> but enforces that there can be at most
/// one thread that accesses a resource.  This functionality could be implemented with a standard
/// "lock" object in .NET but this type has the following additional features:
/// <list type="bullet">
///   <item>
///     <para>
///     If a second thread attempts to enter the barricade, the code outright throws an exception
///     instead of waiting (possibly for a long time) until the first thread is done.  This behavior 
///     helps to identify programming errors, when the object should not be used from multiple threads 
///     in the first place.
///     </para>
///   </item>
///   <item>
///     <para>
///     A thread is not allowed to enter the barricade twice, unlike locks in C# which are re-entrant
///     by default.  Many designs are better off disallowing recursive locks and this library is no
///     exception.
///     </para>
///   </item>
///   <item>
///     <para>
///     This type also tracks disposal: entering the barricade fails if the object being protected
///     has already been disposed.
///     </para>    
///   </item>
///   <item>
///     <para>
///     When some thread successfully enters a barricade, a managed reference to the object being
///     protected is kept so that it cannot be inadvertently finalized by the garbage collector
///     running in another thread.  Thus manual calls to <see cref="GC.KeepAlive(object?)" />
///     are not necessary.
///     </para>
///   </item>
/// </list>
/// </para>
/// <para>
/// This type can be used to implement wrappers around resources from a native library that are
/// <em>not</em> thread-safe, <em>and</em> and should not be used
/// accessed from multiple threads even if that does not crash the native library, because 
/// their basic operation mutates state (so callers have to synchronize anyway to work with
/// consistent states).
/// </para>
/// </remarks>
internal struct Barricade
{
    /// <summary>
    /// State of this barricade (lock).
    /// </summary>
    /// <remarks>
    /// 0: unlocked. -1 disposed. Otherwise: the managed thread ID of the thread that locked this.
    /// </remarks>
    private int _state;

    /// <summary>
    /// Dynamic scope where unique ownership of some resource is to be taken by the current
    /// thread, within an object that uses <see cref="HandleRefCount" /> to control
    /// multi-threaded access.
    /// </summary>
    public ref struct Scope
    {
        private ref int _state;

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
        public Scope(ref Barricade parent, object targetObject)
        {
            _state = ref parent._state;

            int thisThreadId = Environment.CurrentManagedThreadId;
            int oldState = Interlocked.CompareExchange(ref _state, thisThreadId, 0);

            if (oldState == 0)
                return;

            if (oldState < 0)
            {
                throw new ObjectDisposedException(targetObject.GetType().FullName,
                                                 "Cannot operate on a disposed object. ");
            }
            else if (oldState == thisThreadId)
            {
                throw new InvalidOperationException("Attempt to re-enter critical section from the same thread. ");
            }
            else
            {
                throw new InvalidOperationException("Attempt to use this object from multiple threads at the same time. ");
            }
        }

        public void Dispose()
        {
            // "Release" write to publish all other fields in the same managed object
            Volatile.Write(ref _state, 0);

            // Defend against calling Dispose multiple times
            _state = ref Unsafe.NullRef<int>();
        }
    }

    /// <summary>
    /// Prepare to destroy the resources where this <see cref="Barricade" /> is used 
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
        int v = _state;
        int w;
        do
        {
            // Already disposed.
            if (v < 0)
                return false;

            if (v > 0)
                throw new InvalidOperationException("Cannot dispose object while it is being used from another thread. ");

            w = v;
        } while ((v = Interlocked.CompareExchange(ref _state, -1, w)) != w);

        return true;
    }
}
