using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;

namespace Mallard.Interop;

[CustomMarshaller(managedType: typeof(BigInteger),
                  marshalMode: MarshalMode.ManagedToUnmanagedOut,    
                  marshallerType: typeof(BigIntegerMarshaller))]
[CustomMarshaller(managedType: typeof(BigInteger),
                  marshalMode: MarshalMode.ManagedToUnmanagedIn,
                  marshallerType: typeof(BigIntegerMarshaller.ManagedToUnmanagedIn))]
internal static unsafe class BigIntegerMarshaller
{
    public static BigInteger ConvertToManaged(duckdb_bignum input)
    {
        var output = new BigInteger(new ReadOnlySpan<byte>(input.data, (int)input.size),
                                    isUnsigned: true, isBigEndian: false);
        return input.is_negative ? -output : output;
    }

    public ref struct ManagedToUnmanagedIn
    {
        public static int BufferSize { get; } = 0x200;

        private duckdb_bignum _output;
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

        public duckdb_bignum ToUnmanaged() => _output;

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
