using System;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// The representation of a variable-length integer (VARINT) in a DuckDB vector.
/// </summary>
/// <remarks>
/// This structure is only used for reading, not for writing/sending values to DuckDB.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly ref struct DuckDbVarInt
{
    /// <summary>
    /// The blob that stores the VARINT.
    /// </summary>
    private readonly DuckDbBlob _blob;

    /// <summary>
    /// Convert to a .NET <see cref="BigInteger" /> instance.
    /// </summary>
    /// <returns>
    /// <see cref="BigInteger" /> with the same numerical value.
    /// </returns>
    public BigInteger ToBigInteger() => ConvertTo<BigInteger>();

    /// <summary>
    /// Convert to some other binary integer type.
    /// </summary>
    /// <typeparam name="T">Binary integer type, typically with a variable-length representation.
    /// </typeparam>
    /// <returns>
    /// Instance of <typeparamref name="T"/> with the same numerical value.
    /// </returns>
    /// <exception cref="OverflowException">
    /// The given type <typeparamref name="T" /> cannot hold the numerical
    /// value of this variable-length integer from DuckDB.
    /// </exception>
    public T ConvertTo<T>() where T : IBinaryInteger<T>
    {
        var buffer = _blob.AsSpan();

        // The format is not described in the public API documentation but can be 
        // determined by reading DuckDB's source code.  Note that this storage format
        // is different from the type "duckdb_varint" used in the API to communicate
        // single values of VARINT.
        // 
        // The VARINT is stored in the same way as blobs, with a 3-byte header.
        // The 3-byte header is interpreted as a BIG-ENDIAN 24-bit integer.  Following
        // the header are the bytes of the big integer in SMALL-ENDIAN. 
        //
        // Let h be the 24-bit unsigned integer that is the header.
        // Let d be the number of bytes used to represent the magnitude of the VARINT,
        // as a 23-bit unsigned integer zero-extended to 24 bits.
        // 
        // Then:
        //
        // When the VARINT value is positive: h = d | 0x800000                      [MSB is on]
        // When the VARINT value is negative: h = ~(d | 0x800000) = (~d & 0x7FFFFF) [MSB is off]
        //
        // The bytes for the magnitude of the VARINT are also inverted (bitwise-NOT applied)
        // when the VARINT is negative.
        //
        // For details, see the C++ methods in src/common/types/varint.cpp:
        //   std::string Varint::FromByteArray(uint8_t* data, idx_t size, bool is_negative)
        //   void Varint::SetHeader(char* blob, uint64_t number_of_bytes, bool is_negative)

        var h = (buffer[0] << 16) | (buffer[1] << 8) | buffer[2];
        
        bool isPositive = ((h & 0x800000) != 0);
        int numBytes = (isPositive ? h : ~h) & 0x7FFFFF;

        ReadOnlySpan<byte> valueBuffer = buffer.Slice(3, numBytes);

        if (isPositive)
        {
            return T.ReadLittleEndian(valueBuffer, isUnsigned: true);
        }
        else
        {
            // PITA.  We have to create a temporary buffer to do bitwise-NOT on all the bytes.
            //
            // Doing bitwise-NOT in 64-bit groups is not meant to be an optimization
            // in so much as the fact that .NET essentially requires promoting a byte to at least
            // a 32-bit integer before we can apply bitwise-NOT.  If we insist on staying with
            // bytes, then we have insert ugly casts everywhere.
            //
            // In the same vein, we would prefer the indices and sizes in this function to be all
            // unsigned (typed as uint), but the .NET API has rough edges with unsigned quantities,
            // again requiring casts. We pray the JIT compiler can see that numBytes has a 23-bit
            // range (from its definition above), and so "signed" and "unsigned" operations are
            // totally equivalent.
            //
            // Note that BigInteger uses the sign-magnitude representation internally, so
            // converting to twos'-complement representation here would not make the conversion
            // more efficient.

            int numWords = numBytes / sizeof(ulong);
            Span<ulong> magnitudeBuffer = (numWords < 256) ? stackalloc ulong[numWords + 1]
                                                           : new ulong[numWords + 1];
            for (int k = 0; k < numWords; ++k)
                magnitudeBuffer[k] = ~MemoryMarshal.Read<ulong>(valueBuffer[(k * sizeof(ulong))..]);

            ulong lastWord = 0;
            for (int i = 0; i < numBytes % sizeof(ulong); ++i)
                lastWord |= (ulong)(~valueBuffer[numWords * sizeof(ulong) + i] & 0xFF) << (i * 8);
            magnitudeBuffer[numWords] = lastWord;

            return -T.ReadLittleEndian(MemoryMarshal.AsBytes(magnitudeBuffer), isUnsigned: true);
        }
    }

    private static BigInteger ConvertToBigIntegerFromVector(object? state, in DuckDbVectorInfo vector, int index)
        => vector.UnsafeRead<DuckDbVarInt>(index).ToBigInteger();

    internal unsafe static VectorElementConverter VectorElementConverter
        => VectorElementConverter.Create(&ConvertToBigIntegerFromVector);
}
