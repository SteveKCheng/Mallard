using System;
using System.Buffers.Binary;
using System.Collections;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard.Types;

/// <summary>
/// The representation of a bit string in a DuckDB vector.
/// </summary>
/// <remarks>
/// This structure is only used for reading, not for writing/sending values to DuckDB.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly ref struct DuckDbBitString : IStatelesslyConvertible<DuckDbBitString, BitArray>
{
    /// <summary>
    /// The blob that stores the bit string.
    /// </summary>
    private readonly DuckDbBlob _blob;

    /// <summary>
    /// Convert to a .NET <see cref="BitArray" /> instance.
    /// </summary>
    /// <returns>
    /// <see cref="BitArray" /> with the same bits in the same (indexing) order.
    /// </returns>
    public BitArray ToBitArray()
    {
        var buffer = _blob.Span;

        // DuckDB uses a really wacky encoding of bit strings.
        // See src/common/types/bit.cpp.
        //
        // Firstly, the bit string is MIXED-ENDIAN.
        // The order of the bytes is LITTLE-ENDIAN, i.e. lower indices for the
        // bit positions occur earlier in the data byte sequence.  But within 
        // a single byte, the bit position is BIG-ENDIAN, i.e. MSB is the
        // bit with the lowest index.
        //
        // To top it off, when the length of the bit string is not a multiple
        // of 8, the bit string is padded on the LEFT, i.e. the first data byte
        // contains "padding" bits that should be ignored.  Such bits occur
        // starting from the MSB.
        //
        // The first data byte follows the first byte in the blob, which indicates
        // the number of padding bits.  The header byte always exists, even
        // if the length of the bit string is zero.

        int numPaddingBits = buffer[0];

        int len = (buffer.Length - 1) * 8 - numPaddingBits;
        Debug.Assert(numPaddingBits <= 7 && len >= 0);

        var output = new BitArray(len);
        if (len == 0)
            return output;

        // Unfortunately there might be no more efficient ways to initialize BitArray
        // other than to set the bits individually.  BitArray cannot initialize
        // a group of bits using ReadOnlySpan<byte>.  It can take byte[] for the
        // whole bit string, but we really want to avoid allocating GC memory during
        // conversion.
        //
        // We stick to the naïve algorithm until needs prove otherwise.  Users not
        // satisfied with the performance of BitArray can always use the method
        // GetSegment below.

        byte dataByte = buffer[1];
        byte bitMask = (byte)(1u << (7 - numPaddingBits));
        for (int i = 0; i < 8 - numPaddingBits; ++i)
        {
            output.Set(i, (dataByte & bitMask) != 0);
            bitMask >>= 1;
        }

        for (int k = 2; k < buffer.Length; ++k)
        {
            dataByte = buffer[k];
            bitMask = (byte)(1u << 7);
            int pos = (k - 1) * 8 - numPaddingBits;

            for (int j = 0; j < 8; ++j)
            {
                output.Set(pos + j, (dataByte & bitMask) != 0);
                bitMask >>= 1;
            }
        }

        return output;
    }

    /// <summary>
    /// Get the boolean value at the given bit position.
    /// </summary>
    /// <param name="index">
    /// The zero-based index of the bit position.  Bit 0 refers to the left-most bit
    /// when the bit string is written in DuckDB's syntax.
    /// </param>
    /// <returns>
    /// True if the bit is set; false if not.
    /// </returns>
    /// <exception cref="IndexOutOfRangeException">
    /// The index is out of range for the bit string.
    /// </exception>
    public bool GetBit(int index)
    {
        var buffer = _blob.Span;
        int numPaddingBits = buffer[0];

        int len = (buffer.Length - 1) * 8 - numPaddingBits;
        Debug.Assert(numPaddingBits <= 7 && len >= 0);

        if (index < 0 || index >= len)
            throw new IndexOutOfRangeException("Index is out of range for this bit string. ");

        int n = index + numPaddingBits;
        byte dataByte = buffer[1 + (n >> 3)];
        byte bitMask = (byte)(1u << (7 - (n & 7)));

        return (dataByte & bitMask) != 0;
    }

    /// <summary>
    /// The number of bits contained in the bit string.
    /// </summary>
    public int Length
    {
        get
        {
            var buffer = _blob.Span;
            int numPaddingBits = buffer[0];

            int len = (buffer.Length - 1) * 8 - numPaddingBits;
            return len;
        }
    }

    /// <summary>
    /// Copy out consecutive bits in the bit string.
    /// </summary>
    /// <param name="destination">
    /// Buffer to extract the bits into.  The bits will be presented in the same little-endian encoding 
    /// read by <see cref="BitArray.BitArray(byte[])" />: the value of the bit at output position 
    /// <c>i</c> (corresponding to source position <c>i + offset</c>) is 
    /// <c>(destination[i/8] &amp; (1 &lt;&lt; (i%8))) != 0</c>.  Unused bits in the last byte
    /// of the output are always set to zero.  The value of the bytes in the buffer 
    /// beyond the last byte needed for the output will be indeterminate. 
    /// </param>
    /// <param name="offset">
    /// The zero-based index of the position in this bit string to start extracting bits.
    /// Must not be negative.
    /// </param>
    /// <param name="length">
    /// The number of bits to extract.  Must not be negative.
    /// </param>
    /// <returns>
    /// Number of bytes written to the beginning of <paramref name="destination" />.
    /// </returns>
    /// <remarks>
    /// This method is an efficient alternative to converting the data to 
    /// <see cref="BitArray" />.
    /// </remarks>
    /// <exception cref="IndexOutOfRangeException">
    /// The requested range of bits to extract is out of range for this bit string.
    /// </exception>
    public int GetSegment(Span<byte> destination, int offset, int length)
    {
        const int BitsPerByte = 8;
        const int BitsPerWord = BitsPerByte * sizeof(ulong);

        var source = _blob.Span;
        int numPaddingBits = source[0];
        Debug.Assert(numPaddingBits < BitsPerByte);

        int totalSourceBits = BitsPerByte * (source.Length - 1) - numPaddingBits;

        ArgumentOutOfRangeException.ThrowIfLessThan(offset, 0);
        ArgumentOutOfRangeException.ThrowIfLessThan(length, 0);

        if (offset + length > totalSourceBits)
            throw new IndexOutOfRangeException("The given offset plus length extends beyond the end of this bit string. ");

        if (length == 0)
            return 0;

        int countBytes = (length + (BitsPerByte - 1)) / BitsPerByte;
        if (destination.Length < countBytes)
            throw new ArgumentException("The buffer is inadequately sized for the requested output. ");

        // Read a 64-bit word, unaligned from "bytes".  If the buffer is too short, 
        // read the word as if the buffer were extended at the end with bytes of zeroes. 
        static ulong ReadWord(ReadOnlySpan<byte> bytes)
        {
            if (bytes.Length >= sizeof(ulong))
                return BinaryPrimitives.ReadUInt64LittleEndian(bytes);

            ulong v = 0;
            for (int i = 0; i < bytes.Length; ++i)
                v |= (ulong)bytes[i] << (i * BitsPerByte);
            return v;
        }

        // Reverse the bits in each byte within a 64-bit word
        static ulong ReverseBitsInBytes(ulong v)
        {
            v = ((v >> 1) & 0x5555555555555555UL) | ((v & 0x5555555555555555UL) << 1);  // flip adjacent bits
            v = ((v >> 2) & 0x3333333333333333UL) | ((v & 0x3333333333333333UL) << 2);  // flip adjacent pairs of bits
            v = ((v >> 4) & 0x0F0F0F0F0F0F0F0FUL) | ((v & 0x0F0F0F0F0F0F0F0FUL) << 4);  // flip adjacent nibbles
            return v;
        }

        static byte ReverseBitsInByte(byte v)
        {
            uint w = v;
            w = ((w >> 1) & 0x55U) | ((w & 0x55U) << 1);
            w = ((w >> 2) & 0x33U) | ((w & 0x33U) << 2);
            w = ((w >> 4) & 0x0FU) | ((w & 0x0FU) << 4);
            return (byte)w;
        }

        int sourceIndex = offset + numPaddingBits;
        int numSlackBits = sourceIndex & (BitsPerByte - 1);

        // Read first word, and then shift out the bits that are located before desired offset
        source = source[(sourceIndex / BitsPerByte + 1)..]; // +1 is to skip past header byte
        ulong v = ReverseBitsInBytes(ReadWord(source)) >> numSlackBits;

        // Process subsequent words
        int i;
        for (i = sizeof(ulong); i < countBytes; i += sizeof(ulong))
        {
            ulong w = ReverseBitsInBytes(ReadWord(source[i..]));

            // Paste in low bits from current word into high bits of previous word
            // that are missing.  The test (numSlackBits > 0) is necessary because
            // shift amounts in .NET are masked (to (BitsPerWord-1) for ulong values).
            v |= (numSlackBits > 0) ? w << (BitsPerWord - numSlackBits) : 0;

            // Write out the previous word to the output
            BinaryPrimitives.WriteUInt64LittleEndian(destination[(i - sizeof(ulong))..], v);

            // Shift out bits for the next iteration
            v = w >> numSlackBits;
        }

        // Equivalent to (BitsPerWord - (length & (BitsPerWord-1))) & (BitsPerWord - 1)
        int numBitsToMaskOut = (-length) & (BitsPerWord - 1);

        // We need to read one more source byte if the slack bits in the last word are not
        // all getting masked off.  We read only conditionally, to take care of the boundary
        // cases that i is already past the end of the source span, or numSlackBits == 0.
        if (numSlackBits > numBitsToMaskOut)
            v |= (ulong)ReverseBitsInByte(source[i]) << (BitsPerWord - numSlackBits);

        // Mask off unused bits in the last word.
        v &= ulong.MaxValue >> numBitsToMaskOut;

        // Write out the last word
        for (i -= sizeof(ulong); i < countBytes; ++i)
        {
            destination[i] = (byte)(v & 0xFFUL);
            v >>= BitsPerByte;
        }

        return countBytes;
    }

    /// <summary>
    /// Convert a bit string in little-endian format to DuckDB's native mixed-endian format.
    /// </summary>
    /// <param name="input">
    /// The input bytes for the bit string with little-endian byte order (same as the one used
    /// by .NET's <see cref="BitArray" /> class).
    /// </param>
    /// <param name="output">
    /// The output buffer to write the bit string in DuckDB's native format.
    /// It must be sized appropriately by the caller.
    /// The first byte will contain the number of padding bits, followed by the data bytes.
    /// </param>
    /// <param name="bitLength">
    /// The actual number of bits (excluding padding) to use from the beginning of <paramref name="input" />.
    /// Must be non-negative.
    /// </param>
    /// <remarks>
    /// See the body of <see cref="ToBitArray" /> for a description of DuckDB's format for bit strings. 
    /// </remarks>
    internal static void ConvertToNativeRepresentation(ReadOnlySpan<byte> input, Span<byte> output, int bitLength)
    {
        Debug.Assert((bitLength + 7) / 8 == input.Length);
        Debug.Assert(input.Length == output.Length);

        // If bitLength is of the form k + 8n for 0 ≤ k < 7, then
        //   numPaddingBits = (8 - k) mod 8.
        // ("mod 8" is the division remainder that produces results in the range 0 ≤ k < 7.
        //  Note the difference from C#'s operator% for negative operands.)
        // But (8 - k) mod 8 = (8 - (bitLength-8n)) mod 8 = (-bitLength) mod 8.
        int numPaddingBits = (-bitLength) & 7; 
        
        // First byte is always the padding count
        output[0] = (byte)numPaddingBits;
        
        if (input.Length == 0)
            return;

        static uint ReverseBitsInByte(byte value)
        {
            uint v = value;
            v = ((v >> 1) & 0x55U) | ((v & 0x55U) << 1);  // swap adjacent bits
            v = ((v >> 2) & 0x33U) | ((v & 0x33U) << 2);  // swap adjacent pairs
            v = ((v >> 4) & 0x0FU) | ((v & 0x0FU) << 4);  // swap adjacent nibbles
            return v;
        }

        // Example with numPaddingBits = 3
        //              8 - numPaddingBits = 5.
        //
        // Notation: P     : preceding/padding bit
        //           .     : zero bit
        //           [0-7] : bit value at logical position in bit array 
        //
        // Below vectors of bits (in the lowest byte) are written with MSB first.
        //
        // p                   = [ P P P . . . . . ]
        //
        // input               = [ 7 6 5 4 3 2 1 0 ]
        // w                   = [ 0 1 2 3 4 5 6 7 ]
        //
        // w >> numPaddingBits = [ . . . 0 1 2 3 4 ]
        // output              = [ P P P 0 1 2 3 4 ]
        //
        // next p              = [ 5 6 7 . . . . . ]

        uint v = uint.MaxValue;
        for (int i = 0; i < input.Length; i++)
        {
            uint p = (v << (8 - numPaddingBits)) & 0xFFu;
            uint w = ReverseBitsInByte(input[i]);
            output[i + 1] = (byte)((w >> numPaddingBits) | p);
            v = w;
        }
    }

    #if !NET10_OR_GREATER

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "m_array")]
    private static extern ref int[] GetRawDataOfBitArrayPrivate(BitArray bitArray);

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_array")]
    private static extern ref byte[] GetRawDataOfBitArrayPrivateDotNet10(BitArray bitArray);

    /// <summary>
    /// Polyfill for <c>CollectionsMarshal.AsBytes</c> under .NET 9.
    /// </summary>
    internal static Span<byte> GetRawDataOfBitArray(BitArray? value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (Environment.Version.Major < 10)
        {
            int[] array = GetRawDataOfBitArrayPrivate(value);
        
            // N.B. If there is a multi-thread race (from bad user code), array.Length
            //      might be inconsistent with value.Length.  Just make sure the Span
            //      remains in-bounds here, and let user-facing methods in this library
            //      check lengths for consistency later.
            var arrayLength = Math.Min(array.Length, (value.Length + 7) / 8);
        
            return arrayLength > 0
                ? MemoryMarshal.CreateSpan(ref Unsafe.As<int, byte>(ref array[0]), arrayLength)
                : Span<byte>.Empty;
        }

        // We might be executing an assembly targeting .NET 9 under .NET 10,
        // where BitArray's internals changed slightly.  We can remove all these
        // ugly hacks once we target .NET 10 exclusively.
        else
        {
            byte[] array = GetRawDataOfBitArrayPrivateDotNet10(value);
            var arrayLength = Math.Min(array.Length, (value.Length + 7) / 8);
            return array.AsSpan()[0..arrayLength];
        }
    }
    
    #endif

    #region Vector element converter

    static BitArray IStatelesslyConvertible<DuckDbBitString, BitArray>.Convert(ref readonly DuckDbBitString item)
        => item.ToBitArray();

    #endregion
}
