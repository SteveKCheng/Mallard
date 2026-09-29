using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using System.Text;

namespace Mallard.Interop;

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
        if (s is not null) 
            return ConvertToUtf8(s.AsSpan(), out utf8Length, buffer);
        
        utf8Length = 0;
        return null;
    }

    private const int MaxUtf8BytesPerChar = 3;

    public byte* ConvertToUtf8(ReadOnlySpan<char> utf16, out int utf8Length, Span<byte> buffer)
    {
        // Quick check for the common case of small strings that fit into an
        // already-allocated (stack-based) buffer.
        // Comparison uses >= to account for the null terminating byte.
        if ((long)MaxUtf8BytesPerChar * utf16.Length >= buffer.Length)
        {
            // Calculate exact byte count when we might need to allocate memory for,
            // including the null terminating byte.
            int requiredSize = checked(Encoding.UTF8.GetByteCount(utf16) + 1);

            if (requiredSize > buffer.Length)
            {
                Dispose();
                _bigBuffer = (byte*)NativeMemory.Alloc((nuint)requiredSize);
                buffer = new Span<byte>(_bigBuffer, requiredSize);
            }
        }

        utf8Length = Encoding.UTF8.GetBytes(utf16, buffer);

        // Null-terminate
        buffer[utf8Length] = 0;

        // Assumes buffer is pinned or is non-GC memory
        return (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
    }

    /// <summary>
    /// Converts multiple UTF-16 strings to null-terminated UTF-8 strings, to pass as a function
    /// argument to a C library function.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A single memory allocation is used to hold all the UTF-8 strings, placed one after another.
    /// </para>
    /// </remarks>
    /// <param name="utf16Strings">The source UTF-16 strings to convert. </param>
    /// <param name="utf8Strings">An array of pointers to the output UTF-8 strings.  This array
    /// must be able to hold the same number of elements as <paramref name="utf16Strings" />.
    /// </param>
    /// <param name="buffer">
    /// Start of the pinned/fixed pre-allocated buffer to hold the strings, or null if this method should
    /// always allocate. 
    /// </param>
    /// <param name="bufferSize">
    /// The size of the buffer specified by <paramref name="buffer" />.  Must be zero if <paramref name="buffer" />
    /// is null. 
    /// </param>
    public void ConvertStringArrayToUtf8(ReadOnlySpan<string> utf16Strings,
                                         byte** utf8Strings,
                                         byte* buffer,
                                         int bufferSize)
    {
        // Calculate total length of all strings including null terminator, to put
        // into one big buffer
        int totalUtf8Size = 0;
        foreach (var utf16String in utf16Strings)
        {
            int utf8Length = Encoding.UTF8.GetByteCount(utf16String);
            totalUtf8Size = checked(totalUtf8Size + utf8Length + 1);
        }

        if (totalUtf8Size > bufferSize)
        {
            Dispose();
            _bigBuffer = (byte*)NativeMemory.Alloc((nuint)totalUtf8Size);
            buffer = _bigBuffer;
            bufferSize = totalUtf8Size;
        }

        var utf8Span = new Span<byte>(buffer, bufferSize);
        int pos = 0;
        for (int k = 0; k < utf16Strings.Length; ++k)
        {
            utf8Strings[k] = buffer + pos;
            var utf8SpanSlice = utf8Span[pos..];
            int utf8Length = Encoding.UTF8.GetBytes(utf16Strings[k], utf8SpanSlice);
            utf8SpanSlice[utf8Length] = 0;  // null-terminate
            pos += utf8Length + 1;
        }
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
