using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using System.Text;

namespace DuckDB.C_API;

[CustomMarshaller(managedType: typeof(string), 
                  marshalMode: MarshalMode.ManagedToUnmanagedOut, 
                  marshallerType: typeof(Utf8StringMarshallerWithFree))]
internal static unsafe class Utf8StringMarshallerWithFree
{
    public static string ConvertToManaged(byte* p)
    {
        if (p == null)
            return string.Empty;

        try
        {
            var utf8Span = MemoryMarshal.CreateReadOnlySpanFromNullTerminated(p);
            return Encoding.UTF8.GetString(utf8Span);
        }
        finally
        {
            NativeMethods.duckdb_free(p);
        }
    }
}

internal unsafe ref struct Utf8StringConverterState
{
    public const int SuggestedBufferSize = 0x200;
    private byte* _bigBuffer;

    public byte* ConvertToUtf8(string? s, out int utf8Length, Span<byte> buffer)
    {
        if (s is null)
        {
            utf8Length = 0;
            return null;
        }

        const int MaxUtf8BytesPerChar = 3;

        // Quick check for the common case of small strings that fit into an
        // already-allocated (stack-based) buffer.
        // Comparison uses >= to account for the null terminating byte.
        if ((long)MaxUtf8BytesPerChar * s.Length >= buffer.Length)
        {
            // Calculate exact byte count when we might need to allocate memory for,
            // including the null terminating byte.
            int requiredSize = checked(Encoding.UTF8.GetByteCount(s) + 1);

            if (requiredSize > buffer.Length)
            {
                Dispose();
                _bigBuffer = (byte*)NativeMemory.Alloc((nuint)requiredSize);
                buffer = new Span<byte>(_bigBuffer, requiredSize);
            }
        }

        utf8Length = Encoding.UTF8.GetBytes(s, buffer);

        // Null-terminate
        buffer[utf8Length] = 0;

        // Assumes buffer is pinned or is non-GC memory
        return (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
    }

    public void Dispose()
    {
        if (_bigBuffer != null)
        {
            NativeMemory.Free(_bigBuffer);
            _bigBuffer = null;
        }
    }
}

[CustomMarshaller(managedType: typeof(string),
                  marshalMode: MarshalMode.ManagedToUnmanagedOut,
                  marshallerType: typeof(Utf8StringMarshallerWithoutFree))]
internal static unsafe class Utf8StringMarshallerWithoutFree
{
    public static string ConvertToManaged(byte* p)
    {
        if (p == null)
            return string.Empty;

        var utf8Span = MemoryMarshal.CreateReadOnlySpanFromNullTerminated(p);
        return Encoding.UTF8.GetString(utf8Span);
    }
}
