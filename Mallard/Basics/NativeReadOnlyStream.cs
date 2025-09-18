using System;
using System.IO;

namespace Mallard;

/// <summary>
/// A read-only seekable stream that reads from a native memory block.
/// </summary>
internal unsafe sealed class NativeReadOnlyMemoryStream : Stream
{
    private readonly byte* _source;
    private readonly long _length;
    private long _position;
    private bool _disposed;
    private readonly object? _owner;

    /// <summary>
    /// Initializes a new instance to read from the given native memory block.
    /// </summary>
    /// <param name="source">Pointer to the native memory buffer.
    /// Must not be null unless <paramref name="length" /> is zero.
    /// </param>
    /// <param name="length">Size of the buffer in bytes. Must not be negative. </param>
    /// <param name="owner">
    /// Object reference that, if kept alive, ensures that the native memory block
    /// remains valid (at the same location). 
    /// </param>
    public NativeReadOnlyMemoryStream(byte* source, long length, object? owner)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (length != 0)
        {
            ArgumentNullException.ThrowIfNull(source);
            _owner = owner;
        }
        
        _source = source;
        _length = length;
        _position = 0;
    }

    public override bool CanRead => !_disposed;
    public override bool CanSeek => !_disposed;
    public override bool CanWrite => false;

    public override long Length
    {
        get
        {
            ThrowIfDisposed();
            return _length;
        }
    }

    public override long Position
    {
        get
        {
            ThrowIfDisposed();
            return _position;
            
        }
        set => Seek(value, SeekOrigin.Begin);
    }

    public override void Flush()
    {
        // No-op for read-only stream
        ThrowIfDisposed();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        if (offset + count > buffer.Length)
            throw new ArgumentException("The sum of offset and count is larger than the buffer length");

        return Read(buffer.AsSpan().Slice(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        ThrowIfDisposed();

        if (buffer.Length == 0 || _position >= _length)
            return 0;

        // Calculate how many bytes we can actually read
        long bytesAvailable = _length - _position;
        int bytesToRead = (int)Math.Min(buffer.Length, bytesAvailable);

        // Copy from native memory to span
        fixed (byte* destinationPtr = buffer)
        {
            Buffer.MemoryCopy(_source + _position, destinationPtr, buffer.Length, bytesToRead);
        }

        _position += bytesToRead;

        GC.KeepAlive(this);
        return bytesToRead;
    }

    public override int ReadByte()
    {
        ThrowIfDisposed();

        if (_position >= _length)
            return -1;

        byte value = _source[_position];
        _position++;
        return value;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        ThrowIfDisposed();

        long newPosition = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentException("Invalid seek origin", nameof(origin))
        };

        if (newPosition < 0)
            throw new IOException("An attempt was made to move the position before the beginning of the stream");
        
        // Allow seeking beyond the end of the stream (standard Stream behavior)
        _position = newPosition;
        return _position;
    }

    public override void SetLength(long value)
        => throw new NotSupportedException("Cannot set length on a read-only stream");

    public override void Write(byte[] buffer, int offset, int count)
        => throw new NotSupportedException("Cannot write to a read-only stream");

    public override void Write(ReadOnlySpan<byte> buffer)
        => throw new NotSupportedException("Cannot write to a read-only stream");

    public override void WriteByte(byte value)
        => throw new NotSupportedException("Cannot write to a read-only stream");

    protected override void Dispose(bool disposing)
    {
        _disposed = true;
        base.Dispose(disposing);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(NativeReadOnlyMemoryStream));
    }
}
