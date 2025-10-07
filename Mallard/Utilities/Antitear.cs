using System;
using System.Threading;

namespace Mallard;

/// <summary>
/// Wraps a structure to allow it to be read or written atomically (i.e. without tearing)
/// yet without locks.
/// </summary>
/// <typeparam name="T">
/// The structure to read and write atomically.
/// </typeparam>
/// <remarks>
/// Uses a version counter to signal when reads/writes should be re-tried (in a loop).
/// Also known as a "seqlock".
/// </remarks>
internal struct Antitear<T>(T initialValue) where T : struct
{
    /// <summary>
    /// Backing field for <see cref="Value" />.
    /// </summary>
    private T _data = initialValue;

    /// <summary>
    /// Version counter protecting <see cref="_data" />.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Incremented before and after <see cref="_data" /> changes.
    /// Is an odd number while <see cref="_data" /> is being set.  Is an even
    /// number when <see cref="_data"/> has been successfully published.
    /// </para>
    /// </remarks>
    private uint _version;

    /// <summary>
    /// The held data value which may be read/written
    /// from any thread without tearing or conflicts.
    /// </summary>
    public T Value
    {
        get
        {
            var spinWait = new SpinWait();

            while (true)
            {
                var v = Volatile.Read(ref _version);
                if (v % 2 == 0)
                {
                    var d = _data;

                    #if NET10_0_OR_GREATER
                    
                    Volatile.ReadBarrier();
                    if (_version == v)
                        return d;
                    
                    #else
                    
                    // See:
                    // https://stackoverflow.com/questions/56419723/which-of-these-implementations-of-seqlock-are-correct
                    // https://github.com/dotnet/runtime/issues/98837
                    // https://github.com/dotnet/runtime/blob/main/docs/design/specs/Memory-model.md
                    // https://github.com/dotnet/runtime/pull/35597
                    //
                    // We need a Read-Read barrier (as above) but there is no API to do so before .NET 10,
                    // so we add a dummy "release" write and an "acquire" load, hoping that the compiler
                    // does not re-order the accesses, and inserts any necessary fences on architectures
                    // with weaker memory ordering.  According to the literal reading of the .NET memory
                    // model, even this sequence of operations does not strictly guarantee that the
                    // reads will not be re-ordered, but we have no alternative short of an expensive,
                    // full fence (Interlocked.MemoryBarrier).
                    Volatile.Write(ref v, v);

                    if (Volatile.Read(ref _version) == v)
                        return d;
                        
                    #endif
                }

                spinWait.SpinOnce();
            }
        }

        set
        {
            var spinWait = new SpinWait();

            unchecked
            {
                while (true)
                {
                    var v = _version;
                    if (v % 2 == 0 && Interlocked.CompareExchange(ref _version, v + 1, v) == v)
                    {
                        _data = value;
                        Volatile.Write(ref _version, v + 2);
                        return;
                    }

                    spinWait.SpinOnce();
                }
            }
        }
    }
}
