using System;
using System.Collections;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// The representation of a bit string in a DuckDB vector.
/// </summary>
/// <remarks>
/// This structure is only used for reading, not for writing/sending values to DuckDB.
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
public readonly ref struct DuckDbBitString
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
        var buffer = _blob.AsSpan();

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
        // DuckDB's wacky encoding does not help; converting it to the more normal
        // encoding used by BitArray, is not so trivial if we want to try to convert
        // at the level of (groups of) bytes.
        //
        // We stick to the naïve algorithm until needs prove otherwise.

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
        var buffer = _blob.AsSpan();
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
            var buffer = _blob.AsSpan();
            int numPaddingBits = buffer[0];

            int len = (buffer.Length - 1) * 8 - numPaddingBits;
            return len;
        }
    }

    #region Vector element converter

    private static BitArray ConvertToBitArrayFromVector(object? state, in DuckDbVectorInfo vector, int index)
    => vector.UnsafeRead<DuckDbBitString>(index).ToBitArray();

    internal unsafe static VectorElementConverter VectorElementConverter
        => VectorElementConverter.Create(&ConvertToBitArrayFromVector);

    #endregion
}
