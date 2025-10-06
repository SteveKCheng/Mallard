using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using System.Threading;

namespace Mallard;

/// <summary>
/// Asymmetric lock that is optimized for objects that are mostly read from one thread, but
/// can be disposed.
/// </summary>
/// <remarks>
/// <para>
/// In particular, this construct is used to mediate access of native resources which are mostly
/// immutable, and accessed by only one thread at a time, typically for reading.
/// While a truly immutable object would automatically be thread-safe and would not need any locking
/// at all,  there may be occasional operations like disposal (destruction of the native resource) which
/// are not thread-safe and must be protected by a lock.  Then even read-only operations have to
/// follow the same locking protocol which can be undesirable for performance.  
/// </para>
/// <para>
/// Ordinarily speaking, locks (or reference counts, as implemented by
/// <see cref="HandleRefCount" />) are not slow when uncontended, and are sufficient for most
/// purposes.  However, in this library we have workloads like reading from a data table with many cells,
/// where a safe API (without using "ref structs") requires locking for reading every individual cell,
/// yet reading a cell should be cheap if not for the locks
/// (because the cells are simple scalar values), all the while when the data table is not supposed to be
/// accessed concurrently anyway (because the data is presented in a streaming manner).
/// The locking serves no real purpose except to prevent run-time memory corruption if the user misuses
/// the API.
/// </para>
/// <para>
/// In the context of this restricted scenario, <see cref="AsymmetricLock" /> implements
/// the optimization of avoiding any synchronization when the same thread keeps re-locking.
/// Specifically, ordinary locks minimally require at least one "interlocked" instruction
/// and  <see cref="AsymmetricLock" /> tries to avoid even that.
/// The optimization is worth doing when the number of CPU cycles taken by the interlocked operation
/// dominates that taken by the real work (like reading a scalar value off a native
/// memory buffer).  
/// </para>
/// <para>
/// The costs of uncontended interlocked instructions probably come from the implied memory barrier — 
/// e.g. on the x86 architecture, all memory reads afterwards, even of unrelated local variables,
/// get delayed until all memory writes from the CPU's store buffer up to and including the
/// interlocked operation complete (Store-Load re-ordering is prohibited).
/// But this is exactly what can be avoided if only one thread is locking
/// the object.  The trade-off is that passing ownership of the object between threads, i.e. different
/// threads locking the object one after another, becomes more costly than in ordinary locks.
/// </para>
/// <para>
/// In .NET that can always happen, silently, if the object is used in asynchronous code
/// (run on the thread pool), so it must be transparently
/// dealt with (without any explicit API calls from the user to relinquish thread affinity).  But such
/// situations should happen seldomly, compared to the number of times locks have to be taken in
/// order to read many cells off a data table.
/// </para>
/// <para>
/// Very similar optimizations are used in implementations of user-space RCU (Read-Copy-Update;
/// a concurrency paradigm to avoid CPU cache-line contention for objects that are mostly read,
/// and seldomly updated).
/// </para>
/// <para>
/// If the native resource is immutable except for its destruction, it is theoretically possible
/// to disallow explicit disposal and rely solely on the .NET garbage collector to run finalizers.
/// Then all locks can be avoided entirely, but then memory usage can become wildly inefficient if
/// many heavy native resources are created.  So, it may be said that <see cref="AsymmetricLock" />
/// tries to get back close to the levels of performance that can be attained by ignoring memory/resource
/// lifetime management completely.
/// </para>
/// <para>
/// As in RCU, it is theoretically possible to extend the optimization technique used here
/// to allow multiple concurrent reader threads, but that is quite more complicated to implement,
/// and, fortunately, such is not needed in this library.
/// </para>
/// <para>
/// This type is a structure only to avoid extra GC allocation when used to make
/// the controlled object (the object that needs locking) be thread-safe (in its public API).
/// Instances should not be copied around.
/// </para>
/// </remarks>
internal struct AsymmetricLock
{
    /// <summary>
    /// Object representing ownership of <see cref="AsymmetricLock" /> by a particular thread,
    /// allow lock contention to be managed.
    /// </summary>
    /// <para>
    /// In the spirit of RCU, different instances of this class represent "versions" of the lock state,
    /// so that multiple threads can individually see consistent versions of that state
    /// while it gets updated by other contending threads.
    /// </para>
    /// <param name="threadId">
    /// The managed ID of the (target) thread.
    /// </param>
    private sealed class Ticket(int threadId)
    {
        /// <summary>
        /// The unique managed ID of the target thread.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This ID is used to quickly determine if the instance of this class
        /// posted in <see cref="AsymmetricLock._activeTicket" />
        /// targets the currently running thread (which always knows its own ID
        /// through <see cref="Environment.CurrentManagedThreadId" />).
        /// </para>
        /// <para>
        /// This object does not validly represent a thread if this ID is zero
        /// or negative.
        /// </para> 
        /// </remarks>
        internal readonly int ThreadId = threadId;
        
        private bool _isLocked;

        /// <summary>
        /// Synchronously wait for the target thread to stop using the object
        /// controlled by <see cref="AsymmetricLock" />. 
        /// </summary>
        /// <remarks>
        /// <para>
        /// The whole point of <see cref="AsymmetricLock" /> is to avoid
        /// calling synchronization APIs or executing synchronizing CPU instructions
        /// on the fast path, so the only alternative for another thread
        /// to wait for the target thread is to spin.
        /// </para>
        /// <para>
        /// It is a bug to call this method when the current thread is the
        /// target thread of this ticket (which would deadlock).
        /// In Debug builds, this method will throw for that erroneous condition. 
        /// </para>
        /// </remarks>
        internal void SpinWait()
        {
            Debug.Assert(ThreadId > 0 && ThreadId != Environment.CurrentManagedThreadId);
            
            // If the _isLocked flag is false, we still cannot rule out that the other
            // thread could be racing to (try to) re-take the lock, and this thread has
            // not seen the other thread's write to the flag yet.  We can only know for
            // sure after executing memory barriers on both sides.
            //
            // If the flag is (seen to be) true already, there is no ambiguity, and
            // we can skip the expensive process-wide barrier.
            if (!_isLocked)
            {
                Interlocked.MemoryBarrierProcessWide();
                
                // We are on the slow path anyway, so for completeness
                // just make sure entering the lock is an "acquire" operation
                // with regards to the memory contents of the object being locked.
                //
                // Note that the other thread could race to write some data, and then
                // set _isLocked (to false), precisely after the process-wide memory
                // barrier (above) executes, so this "acquire" is not (perfectly) redundant.
                //
                // The same comments apply to the second Volatile.Read below. 
                if (!Volatile.Read(ref _isLocked))
                    return;
            }

            // _isLocked is true right now.  Keep polling until the other thread
            // unlocks, and thus flips it to false.
            var spinWait = new SpinWait();
            do
            {
                spinWait.SpinOnce();
            } while (Volatile.Read(ref _isLocked));
        }

        /// <summary>
        /// Sets a flag marking that the target thread is using the object
        /// controlled by <see cref="AsymmetricLock" />.
        /// </summary>
        internal void Lock()
        {
            AssertNotLocked();
            _isLocked = true;
        }

        /// <summary>
        /// Mark that the target thread has (temporarily) finished using the object
        /// controlled by <see cref="AsymmetricLock" />.
        /// </summary>
        internal void Unlock() => _isLocked = false;

        /// <summary>
        /// In Debug builds, asserts that this ticket has not been marked as
        /// locked (by the target thread).
        /// </summary>
        internal void AssertNotLocked() => Debug.Assert(ThreadId > 0 && !_isLocked);
    }

    /// <summary>
    /// Ticket object for the thread that may have the object controlled by this instance
    /// locked.
    /// </summary>
    /// <remarks>
    /// <para>
    /// While 2 or more threads are contending for the lock, this field can (of course) indicate only
    /// one thread owns the object.  The other threads must wait or back off.
    /// </para>
    /// <para>
    /// The initial value of this variable is null, meaning no thread has entered the lock.
    /// Once a thread has entered the lock, it generally does not reset this variable, allowing
    /// it to quickly re-lock, re-using the same ticket. 
    /// </para>
    /// <para>
    /// When the controlled object is to be (marked as) disposed, this variable is set to
    /// <see cref="TrashTicket" />.  That is a terminal value; this variable cannot change to any
    /// other value afterwards.
    /// </para>
    /// </remarks>
    private Ticket? _activeTicket;

    /// <summary>
    /// Sentinel that marks the object controlled by this lock as disposed.
    /// </summary>
    private static readonly Ticket TrashTicket = new Ticket(-1);

    /// <summary>
    /// Initialize to an unlocked state. 
    /// </summary>
    public AsymmetricLock()
    {
    }

    private static void ThrowForDisposed(Ticket? ticket, object? targetObject)
    {
        if (object.ReferenceEquals(ticket, TrashTicket))
            throw new ObjectDisposedException(targetObject?.GetType().Name, "The object is already disposed. ");
    }

    /// <summary>
    /// Erects a compiler-level fence that prevents
    /// re-ordering of memory operations across the fence.
    /// </summary>
    /// <remarks>
    /// Unfortunately, .NET does not come with a built-in API for this low-level
    /// niche scenario, so this method imitates the desired effect.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CompilerFence()
    {
        #if NET10_OR_GREATER
        // Test if this code is running on the x86 (or x86-64) CPU architecture.
        // If so, Volatile.ReadBarrier does not generate any CPU-level memory barrier,
        // which is exactly what we want.
        //
        // Support for SSE is tested as a proxy, because the MSIL compiler may
        // not always optimize tests on RuntimeInformation.OSArchitecture to
        // constants at run-time.
        if (Sse.IsSupported)
        {
            Volatile.ReadBarrier();
            return;
        }
        #endif

        // For other CPU architectures, block re-ordering of memory operations
        // by interposing a dummy function.  Obviously this does not result
        // in the most efficient machine code, but we would rather play it safe,
        // than rely on the fact that the MSIL compiler right now just happens
        // to not re-order memory accesses if we truly do nothing here (and
        // that assumption being silently broken would be very hard to
        // regression-test against).
        //
        // This code pattern can also be seen in Microsoft's code:
        // https://github.com/dotnet/runtime/blob/8167e996bf816d375f21b9b045442906e673c7e9/src/libraries/System.Threading/tests/InterlockedTests.cs#L1124
        // https://github.com/dotnet/corert/blob/master/src/System.Private.CoreLib/src/System/Threading/ObjectHeader.cs
        // 
        // There is a proposal for .NET to add an API to do exactly what we want
        // but it remains unimplemented:
        //   https://github.com/dotnet/runtime/issues/75874
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void NoOp() {}
        NoOp();
    }

    /// <summary>
    /// Have the current thread enter the lock. 
    /// </summary>
    /// <returns>
    /// The ticket for the current thread which is needed for unlocking afterwards.
    /// </returns>
    private Ticket Lock(object? targetObject)
    {
        Ticket? oldTicket = _activeTicket;

        int myThreadId = Environment.CurrentManagedThreadId;
        Ticket? myTicket = null;

        // Fast path: the same thread is taking the lock over and over again
        if (oldTicket != null && oldTicket.ThreadId == myThreadId)
        {
            myTicket = oldTicket;
            myTicket.Lock();

            // This compiler fence effectively gets upgraded to a full memory
            // barrier (Interlocked.MemoryBarrier) when WaitForPrecedingOwner
            // calls Interlocked.MemoryBarrierProcessWide, i.e. only
            // when there is contention for the lock.
            //
            // The upgraded barrier ensures sequential consistency between "locking"
            // myTicket above, which just sets the _isLocked flag (in a "relaxed" manner),
            // and the following check (which reads from _activeTicket).
            // On the hardware level, it ensures the write to the flag
            // is flushed from the CPU's store buffer to be globally
            // visible to the other contending threads.
            //
            // Obviously this code optimizes for the case where the same
            // thread is taking this lock, so that executing a full memory
            // barrier (or interlocked operation) unconditionally would
            // just waste (~20 to 40) CPU cycles.
            //
            // This is a well-known technique from implementations of RCU
            // in user-space programs.  (Interlocked.MemoryBarrierProcessWide
            // tells the OS kernel to issue inter-processor interrupts that
            // make all running threads execute memory barriers.)
            //
            // The comments to the patch to the Linux kernel that introduced the
            // underlying system call on that OS explains this technique really well:
            //   https://lkml.org/lkml/2010/4/18/15
            //   https://lkml.org/lkml/2015/7/10/766
            //
            // And some historical background in the context of .NET:
            //   https://github.com/dotnet/runtime/issues/3952
            //   https://github.com/dotnet/runtime/issues/20500
            //
            // A performance bug in .NET, involving native-resource lifetime
            // management, that demonstrates the locking overhead is real:
            //   https://github.com/dotnet/runtime/issues/114846
            CompilerFence();

            // "Double-checked locking" after we "locked" our ticket.
            //
            // If another thread stole the lock from under this thread, then
            // either that thread sees that we have locked our ticket above
            // (and will spin-wait), or this thread will detect that other thread
            // having changed _activeTicket, and back off from the fast path.
            oldTicket = _activeTicket;
            if (oldTicket == myTicket)
                return myTicket;
        }

        // Slow path: need to change the active ticket, possibly
        // seizing ownership of the lock from another thread
        try
        {
            ThrowForDisposed(oldTicket, targetObject);

            // Create a new ticket to represent this thread.  Again, "lock" our ticket
            // first so if yet another thread steals ownership from this thread,
            // we can manage the contention.
            myTicket = new Ticket(myThreadId);
            myTicket.Lock();

            // Atomically update _activeTicket, getting the immediately preceding ticket,
            // and ensuring the controlled object has not been disposed first.
            while (true)
            {
                Ticket? comparand = oldTicket;
                oldTicket = Interlocked.CompareExchange(ref _activeTicket, myTicket, comparand);
                if (oldTicket == comparand)
                    break;
            
                ThrowForDisposed(oldTicket, targetObject);
            }
            
            // We have "stolen" the lock (according to _activeTicket), but the preceding thread
            // that had locked cannot know about that, and so we must wait for it to finish.
            //
            // Note that the hand-over of owners respects the total modification order of
            // _activeTicket, and ticket objects are NOT re-used once they have been "de-activated",
            // so even with many threads contending we will not introduce cycles in spin-waiting,
            // i.e. deadlocks.
            oldTicket?.SpinWait();

            return myTicket;
        }
        finally
        {
            myTicket?.Unlock();
        }
    }

    /// <summary>
    /// Prepare to destroy the resources whose access is being mediated by this instance. 
    /// </summary>
    /// <returns>
    /// True if the resource is now ready for disposal.  False if it is already disposed.
    /// </returns>
    /// <remarks>
    /// The flag for disposal is atomically flipped, so at most one thread will see a true value
    /// from a call to this method.
    /// Thus, disposal code that is guarded by the return value will not race between different threads.
    /// </remarks>
    public bool PrepareToDisposeOwner()
    {
        var oldTicket = Interlocked.Exchange(ref _activeTicket, TrashTicket);
        
        // Already disposed
        if (oldTicket == TrashTicket)
            return false;
        
        // Another thread holds the lock on the controlled object.
        // We must wait for it to release the lock.
        if (oldTicket != null && oldTicket.ThreadId != Environment.CurrentManagedThreadId)
            oldTicket.SpinWait();

        // oldTicket, if not null, is actually for this thread.
        // Make sure we are not inside our own lock.
        else
            oldTicket?.AssertNotLocked();
            
        return true;
    }

    /// <summary>
    /// Lock scope for <see cref="AsymmetricLock" />.
    /// </summary>
    public readonly ref struct Scope
    {
        private readonly Ticket _ticket;
        internal Scope(scoped ref AsymmetricLock parent, object? targetObject) => _ticket = parent.Lock(targetObject);
        
        /// <summary>
        /// Exit the lock.
        /// </summary>
        /// <remarks>
        /// <see cref="AsymmetricLock" /> is intended for and optimized for mostly immutable objects.
        /// Unlike almost all other kinds of locks, unlocking <see cref="AsymmetricLock" />
        /// does not imply a "release" operation, i.e. a write barrier.  If a write barrier is
        /// needed, the caller may erect one explicitly.
        /// Note that taking a lock (previously owned by another thread, through <see cref="EnterScope" />)
        /// does imply an "acquire" operation, i.e. a read barrier. 
        /// </remarks>
        public void Dispose() => _ticket.Unlock();
    }

    /// <summary>
    /// Establish a dynamic scope where the controlled object is to be locked for access.
    /// </summary>
    /// <param name="targetObject">
    /// The object being controlled by the lock,
    /// used only for reporting errors when the lock cannot be entered.
    /// </param>
    /// <exception cref="ObjectDisposedException">
    /// The object controlled by this instance has been indicated to be disposed 
    /// (or is in the middle of being disposed by another thread).
    /// </exception>
    /// <returns>Scope object that should be the subject of a <c>using</c> statement in C#,
    /// exiting the lock on disposal.
    /// </returns>
    public Scope EnterScope(object? targetObject) => new Scope(ref this, targetObject);
}
