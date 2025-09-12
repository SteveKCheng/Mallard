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
                    #else
                    // Dummy write to prevent re-ordering of read of d to follow the
                    // verifying read of _version below.
                    Volatile.Write(ref v, v);
                    #endif
                    
                    if (_version == v)
                        return d;
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
