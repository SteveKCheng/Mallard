using System;
using System.Collections;
using System.Linq;
using System.Numerics;
using Xunit;
using TUnit.Core;

namespace Mallard.Tests;

public class TestExecuteScalar
{
    [Test]
    public void Decimal()
    {
        using var dbConn = new DuckDbConnection("");

        // This number cannot be represented exactly in floating-point
        decimal v = 0.1M;

        var ps = dbConn.PrepareStatement("SELECT $1");
        ps.Parameters[1].Set(v);
        object? v_out = ps.ExecuteScalar();

        Assert.IsType<Decimal>(v_out);
        Assert.StrictEqual(v, (decimal)v_out);
    }

    [Test]
    public void BigInteger()
    {
        using var dbConn = new DuckDbConnection("");

        using var ps = dbConn.PrepareStatement("SELECT $1");
        Span<byte> buffer = stackalloc byte[1024];

        void Check(Span<byte> bytes, bool isNegative)
        {
            var magnitude = new BigInteger(bytes, isUnsigned: true, isBigEndian: false);
            var value = isNegative ? -magnitude : magnitude;

            ps.Parameters[1].Set(value);

            object? valueOut = ps.ExecuteScalar();
            Assert.IsType<BigInteger>(valueOut);
            Assert.StrictEqual(value, (BigInteger)valueOut);
        }

        // Try different values
        foreach (int q in new[] { 127, 131, 163, 521, -127, -131, -163, -521 })
        {
            int p = Math.Abs(q);

            // Mersenne number 2^p - 1 
            // For example, p = 131, the number in decimal is:
            //   2722258935367507707706996859454145691647 (40 digits)

            buffer[..(p >> 3)].Fill(0xFF);
            buffer[(p >> 3)] = (byte)((1u << (1 + (p & 7))) - 1);

            int len = (p >> 3) + 1;
            buffer[len..].Clear();

            Check(buffer[..len], q < 0);
        }

        // The Mersenne numbers have a simple pattern in memory that is
        // easy to recognize when debugging.  Now try some complex cases with
        // "random" bytes to ensure that we are not interpreting the bytes backwards, etc.
        var random = new Random(Seed: 37);
        foreach (int p in new[] { 16, 32, 64, 128, 256 })
        {
            int len = (p >> 3) + 1;
            random.NextBytes(buffer[..len]);
            buffer[(p >> 3)] &= (byte)((1u << (1 + (p & 7))) - 1);
            buffer[len..].Clear();

            Check(buffer[..len], false);
        }
    }

    [Test]
    public void BitString()
    {
        using var dbConn = new DuckDbConnection("");
        using var ps = dbConn.PrepareStatement("SELECT $1::BITSTRING");

        Span<byte> buffer = stackalloc byte[512];
        var random = new Random(Seed: 37);

        // Generate bit strings of various lengths to test
        foreach (int len in new[] { 1, 4, 7, 8, 15, 16, 1023, 1024, 1025 })
        {
            int numBytes = (len + 7) / 8;
            random.NextBytes(buffer[..numBytes]);

            // Mask off bits beyond the logical end of the bit string.
            // Does nothing if (len & 7) == 0, because shift amounts are masked,
            // which is what we want.
            buffer[numBytes - 1] &= (byte)(uint.MaxValue >> (32 - (len & 7)));

            var bitArray = new BitArray(buffer[..numBytes].ToArray());

            // Create a string for the value to send into SQL
            ps.Parameters[1].Set(CreateStringFromBitArray(bitArray, 0, len));

            var bitArray2 = ps.ExecuteValue<BitArray>();
            Assert.NotNull(bitArray2);

            // BitArray does not do structural equality, so convert to IEnumerable<bool> first.
            Assert2.Equal(bitArray.Cast<bool>().Take(len), bitArray2.Cast<bool>());
        }
    }
    
    internal static string CreateStringFromBitArray(BitArray a, int start, int length)
        => string.Create(length, (a, start), static (buffer, state) =>
        {
            (BitArray a, int start) = state;
            for (int i = 0; i < buffer.Length; ++i)
                buffer[i] = a[i + start] ? '1' : '0';
        });

    [Test]
    public void IntegerPromotion()
    {
        using var dbConn = new DuckDbConnection("");

        using var ps = dbConn.PrepareStatement("SELECT $1::TINYINT");
        sbyte value = 9;
        ps.Parameters[1].Set(value);
        
        Assert.Equal(value, ps.ExecuteValue<sbyte>());
        Assert.Equal(value, ps.ExecuteValue<short>());
        Assert.Equal(value, ps.ExecuteValue<int>());
        Assert.Equal(value, ps.ExecuteValue<long>());
        Assert.Equal(value, ps.ExecuteValue<Int128>());

        // FIXME decide on the exact type of Exception to throw
        Assert.ThrowsAny<Exception>(() => ps.ExecuteValue<byte>());
        Assert.ThrowsAny<Exception>(() => ps.ExecuteValue<ushort>());
        Assert.ThrowsAny<Exception>(() => ps.ExecuteValue<uint>());
        Assert.ThrowsAny<Exception>(() => ps.ExecuteValue<ulong>());
        Assert.ThrowsAny<Exception>(() => ps.ExecuteValue<UInt128>());
    }

    [Test]
    public void EnumPromotion()
    {
        using var dbConn = new DuckDbConnection("");

        using var ps = dbConn.PrepareStatement("SELECT $1::ENUM ('すごい', '楽しい', '面白くない', 'まずい')");
        byte value = 1;
        ps.Parameters[1].Set("楽しい");
        
        Assert.Equal(value, ps.ExecuteValue<byte>());
        Assert.Equal(value, ps.ExecuteValue<ushort>());
        Assert.Equal(value, ps.ExecuteValue<uint>());
    }

    [Test]
    public void BlobParameter()
    {
        using var dbConn = new DuckDbConnection("");
        using var ps = dbConn.PrepareStatement("SELECT $1::BLOB");

        var sourceBuffer = new byte[512];
        new Random(Seed: 37).NextBytes(sourceBuffer);
        
        ps.Parameters[1].Set(sourceBuffer);
        var destBuffer = ps.ExecuteValue<byte[]>();
        
        Assert2.Equal(sourceBuffer, destBuffer);
    }
}
