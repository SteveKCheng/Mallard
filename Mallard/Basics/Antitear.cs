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
/// Uses a version counter to signal when reads/writes should be re-tried. 
/// </remarks>
internal struct Antitear<T> where T : struct
{
    private T _data;
    private uint _version;

    /// <summary>
    /// The held value of type <typeparamref name="T" /> which is read/written atomically.
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

                    // Dummy write to prevent re-ordering of read of d to follow the
                    // verifying read of _version below.
                    Volatile.Write(ref v, v);

                    // Volatile.Read may not be necessary here but we code defensively,
                    // and there is no real (performance) downside to it.
                    if (Volatile.Read(ref _version) == v)
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
