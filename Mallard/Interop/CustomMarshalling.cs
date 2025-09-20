using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using System.Text;

namespace Mallard.Interop;
using Mallard.Types;

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

[CustomMarshaller(managedType: typeof(BigInteger),
                  marshalMode: MarshalMode.ManagedToUnmanagedOut,    
                  marshallerType: typeof(BigIntegerMarshaller))]
[CustomMarshaller(managedType: typeof(BigInteger),
                  marshalMode: MarshalMode.ManagedToUnmanagedIn,
                  marshallerType: typeof(BigIntegerMarshaller.ManagedToUnmanagedIn))]
internal static unsafe class BigIntegerMarshaller
{
    public static BigInteger ConvertToManaged(duckdb_varint input)
    {
        var output = new BigInteger(new ReadOnlySpan<byte>(input.data, (int)input.size),
                                    isUnsigned: true, isBigEndian: false);
        return input.is_negative ? -output : output;
    }

    public ref struct ManagedToUnmanagedIn
    {
        public static int BufferSize { get; } = 0x200;

        private duckdb_varint _output;
        private byte* _extraBuffer;

        public void FromManaged(BigInteger input, Span<byte> buffer)
        {
            // Abs(BigInteger) exists only since .NET 10.
            // TryWriteBytes below does not allow negative inputs when writing
            // in the "unsigned encoding", even though internally it uses
            // signed-magnitude encoding (except for small inputs).
            //
            // Fortunately the latter fact means that flipping the sign of
            // BigInteger is a cheap operation, and does not require re-encoding
            // the number into newly-allocated memory (as would be required if
            // twos-complement encoding was used).
            //
            // See https://github.com/dotnet/runtime/blob/release/8.0/src/libraries/System.Runtime.Numerics/src/System/Numerics/BigInteger.cs
            // for details.
            var absInput = (input.Sign < 0) ? -input : input;

            var byteCount = absInput.GetByteCount(isUnsigned: true);
            if (byteCount > buffer.Length)
            {
                _extraBuffer = (byte*)NativeMemory.Alloc((nuint)byteCount);
                buffer = new Span<byte>(_extraBuffer, byteCount);
            }

            absInput.TryWriteBytes(buffer, out var bytesWritten, 
                                   isUnsigned: true, isBigEndian: false);
            
            _output.data = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(buffer));
            _output.size = bytesWritten;
            _output.is_negative = (input.Sign < 0);
        }

        public duckdb_varint ToUnmanaged() => _output;

        public void Free()
        {
            if (_extraBuffer != null)
            {
                NativeMemory.Free(_extraBuffer);
                _extraBuffer = null;
            }
        }
    }
}

//
// Custom marshallers for UInt128, Int128 to work around bug in .NET runtime.
// See comment surrounding struct DuckDbHugeUInt.
//

[CustomMarshaller(managedType: typeof(Int128), marshalMode: MarshalMode.Default, marshallerType: typeof(Int128Marshaller))]
internal static class Int128Marshaller
{
    public static Int128 ConvertToManaged(DuckDbUInt128 v) => v.ToInt128();
    public static DuckDbUInt128 ConvertToUnmanaged(Int128 v) => new(v);
}

[CustomMarshaller(managedType: typeof(UInt128), marshalMode: MarshalMode.Default, marshallerType: typeof(UInt128Marshaller))]
internal static class UInt128Marshaller
{
    public static UInt128 ConvertToManaged(DuckDbUInt128 v) => v.ToUInt128();
    public static DuckDbUInt128 ConvertToUnmanaged(UInt128 v) => new(v);
}
