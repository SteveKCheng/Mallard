using System;
using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Text.Unicode;

namespace Mallard;
using Mallard.Types;

/// <summary>
/// Provides access to a column of <see cref="DuckDbResultChunk" />.
/// </summary>
/// <remarks>
/// <para>
/// This type essentially provides the functionality of <see cref="DuckDbVectorReader{T}" />,
/// but without incorporating the vector element type as a generic parameter,
/// and without the restrictions of "ref structs".  It is necessary
/// to fit some API shapes, in particular ADO.NET.  Naturally, it has worse performance
/// characteristics (in order to maintain the same level of memory safety), 
/// so <see cref="DuckDbVectorReader{T}" /> remains the preferred
/// approach to reading DuckDB vectors.
/// </para>
/// <para>
/// The word "delegate" in the name of this type refers to its instances being like
/// delegates (in the .NET sense of the term) to the chunk's column/vector.
/// </para>
/// <para>
/// Because the .NET type for the vector elements does not get specified (at construction), instances
/// of this class assume the default type (mapping) as decided by this library.
/// </para>
/// </remarks>
public class DuckDbVectorDelegateReader : IDuckDbVector
{
    /// <summary>
    /// Holds on to reference to parent to prevent garbage collection while this instance
    /// still exists.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This object is the originating <see cref="DuckDbResult" />, but since it does not
    /// need to be directly accessed again, this field is typed as <see cref="object" />.
    /// </para>
    /// <para>
    /// Because <see cref="DuckDbVectorInfo" /> is passed by reference into 
    /// <see cref="VectorElementConverter.Convert{T}(in DuckDbVectorInfo, int, bool)" />,
    /// calls to <see cref="GC.KeepAlive(object?)" /> ought to be not needed during conversion.
    /// But there is a danger the .NET IL compiler would optimize out the references so
    /// we put in those calls anyway.
    /// </para>
    /// </remarks>
    private readonly object _owner;

    /// <summary>
    /// Points to one of the originating chunk's columns.
    /// </summary>
    private readonly DuckDbVectorInfo _vector;

    /// <summary>
    /// Converter for the defaulted .NET type for the DuckDB column.
    /// </summary>
    private readonly VectorElementConverter _converter;

    /// <summary>
    /// Converter for the boxed version of the defaulted .NET type for the DuckDB column.
    /// </summary>
    private readonly VectorElementConverter _boxedConverter;

    /// <summary>
    /// Copy of <see cref="IDuckDbVector.ValidityMask" /> of <see cref="_vector"/>, 
    /// allocated in managed memory.
    /// </summary>
    private ulong[]? _validityMaskCopy;

    /// <summary>
    /// Obtains read access to a column (vector) in a result chunk coming from DuckDB.
    /// </summary>
    /// <param name="chunk">
    /// The target chunk.  To enforce memory safety (in the presence of potentially
    /// multi-threaded access), the chunk can no longer be explicitly disposed
    /// once any reference to a column is taken using this class.  Subsequent calls
    /// to <see cref="DuckDbResultChunk.Dispose" /> will be silently ignored.
    /// </param>
    /// <param name="columnIndex">
    /// The index of the desired column.  Must be between 0 (inclusive)
    /// and <see cref="DuckDbResultChunk.ColumnCount" /> (exclusive).
    /// </param>
    public DuckDbVectorDelegateReader(DuckDbResultChunk chunk, int columnIndex)
    {
        chunk.IgnoreDisposals();
        _owner = chunk;
        _vector = chunk.UnsafeGetColumnVector(columnIndex);
        _converter = chunk.GetColumnConverter(columnIndex, null).BindToVector(_vector);
        _boxedConverter = chunk.GetColumnConverter(columnIndex, typeof(object)).BindToVector(_vector);
    }

    /// <summary>
    /// The variable-length bit mask indicating which elements in the vector are valid (not null).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike the efficient implementation from <see cref="DuckDbVectorReader{T}" />,
    /// this method must copy out the array of bits.  The original array is in memory natively
    /// allocated by DuckDB, and this class (not being a ref struct) 
    /// has no way to control the lifetime of the span were the original array to be returned.
    /// </para>
    /// <para>
    /// Clients that use <see cref="DuckDbVectorDelegateReader" /> instead of <see cref="DuckDbVectorReader{T}" />
    /// generally are not using performant span-based APIs anyway, so this fallback exists
    /// only to complete the implementation of the interface method (<see cref="IDuckDbVector.ValidityMask" />).
    /// </para>
    /// </remarks>
    ReadOnlySpan<ulong> IDuckDbVector.ValidityMask
    {
        get
        {
            var m = _validityMaskCopy;

            if (_validityMaskCopy == null)
            {
                var s = _vector.ValidityMask;
                if (s.Length == 0)
                {
                    m = Array.Empty<ulong>();
                }
                else
                {
                    m = new ulong[s.Length];
                    s.CopyTo(m);
                }

                GC.KeepAlive(this);
                _validityMaskCopy = m;
            }

            return new ReadOnlySpan<ulong>(m);
        }
    }

    /// <inheritdoc cref="IDuckDbVector.Length" />
    public int Length => _vector.Length;

    /// <inheritdoc cref="IDuckDbVector.ColumnInfo" />
    public DuckDbColumnInfo ColumnInfo => _vector.ColumnInfo;

    /// <inheritdoc cref="IDuckDbVector.IsItemValid(int)" />
    public bool IsItemValid(int index)
    {
        var b = _vector.IsItemValid(index);
        GC.KeepAlive(this);
        return b;
    }

    /// <summary>
    /// The .NET type that the elements in the DuckDB column are mapped to.
    /// </summary>
    public Type ElementType => _converter.TargetType;

    /// <summary>
    /// Get an item in the vector, cast into <see cref="System.Object"/>, or null if
    /// the selected item is invalid.
    /// </summary>
    /// <param name="rowIndex">The (row) index of the element of the vector. </param>
    public object? GetObjectOrNull(int rowIndex)
    {
        var v = _boxedConverter.Convert<object>(_vector, rowIndex, requireValid: false);
        GC.KeepAlive(this);
        return v;
    }

    /// <summary>
    /// Get an item in the vector, cast into <see cref="System.Object"/>.
    /// </summary>
    /// <param name="rowIndex">The (row) index of the element of the vector. </param>
    public object GetObject(int rowIndex)
    {
        var v = _boxedConverter.Convert<object>(_vector, rowIndex, requireValid: true)!;
        GC.KeepAlive(this);
        return v;
    }

    /// <summary>
    /// Throws the exception for the generic parameter to <see cref="GetValue" /> being wrong.
    /// </summary>
    private void ThrowExceptionForWrongType(Type receiverType)
    {
        throw new ArgumentException(
            "The generic type T that GetValue<T> has been called with is incompatible with the " +
            "actual type of the element from the DuckDB vector. " +
            $"Desired type: {receiverType}, Actual type: {_converter.TargetType}");
    }

    /// <summary>
    /// Get an item in the vector in its default .NET type (without boxing it).
    /// </summary>
    /// <typeparam name="T">
    /// <para>
    /// The .NET type of the column, required to be specified since it has been 
    /// "type-erased" from the type identity of this class.  However, it must 
    /// still match, at run-time, the actual type that the elements in the DuckDB
    /// column have been mapped to by default, as indicated by <see cref="ElementType" />.
    /// (For reference types, a base class or interface can also match.)
    /// </para>
    /// <para>
    /// This type should not be a nullable value type; it would never match
    /// <see cref="ElementType" />.  There is no "notnull" constraint 
    /// on <typeparamref name="T" /> only because some APIs that would be
    /// implemented with this class do not have that shape.
    /// </para>
    /// </typeparam>
    public T GetValue<T>(int rowIndex)
    {
        ref readonly VectorElementConverter converter = ref _boxedConverter;
        
        if (typeof(T) != typeof(object))
        {
            converter = ref _converter;
            if (!typeof(T).IsAssignableWithoutBoxingFrom(converter.TargetType))
                ThrowExceptionForWrongType(typeof(T));
        }

        var v = converter.Convert<T>(_vector, rowIndex, requireValid: true)!;
        GC.KeepAlive(this);
        return v;
    }
    
    #region Specialized methods to read blobs and strings

    /// <summary>
    /// Get a read-only stream over the bytes of a blob, string, or bit string.
    /// </summary>
    /// <param name="rowIndex">
    /// The (row) index of the element that is a blob, string or bit string.
    /// </param>
    public unsafe Stream GetByteStream(int rowIndex)
    {
        var valueKind = ColumnInfo.ValueKind;
        if (valueKind == DuckDbValueKind.Blob ||
            valueKind == DuckDbValueKind.VarChar ||
            valueKind == DuckDbValueKind.Bit)
        {
            var reader = new DuckDbVectorRawReader<DuckDbBlob>(_vector);
            var blob = reader.GetItem(rowIndex);

            fixed (byte* p = blob.Span)
            {
                // NativeReadOnlyMemoryStream captures the pointer which 
                // is legal only because blob.Span is guaranteed to be from
                // native memory owned by _owner.
                return new NativeReadOnlyMemoryStream(p, blob.Span.Length, _owner);
            }
        }
        else
        {
            throw new InvalidOperationException(
                "Cannot use GetByteStream on this element type in a DuckDB vector. ");
        }
    }

    /// <summary>
    /// Get the length in bytes of a blob, string or bit string.
    /// </summary>
    /// <param name="rowIndex">
    /// The (row) index of the element that is a blob, string or bit string.
    /// </param>
    public int GetByteLength(int rowIndex)
    {
        var valueKind = ColumnInfo.ValueKind;
        if (valueKind == DuckDbValueKind.Blob ||
            valueKind == DuckDbValueKind.VarChar ||
            valueKind == DuckDbValueKind.Bit)
        {
            var reader = new DuckDbVectorRawReader<DuckDbBlob>(_vector);
            var blob = reader.GetItem(rowIndex);
            return blob.Span.Length;
        }
        else
        {
            throw new InvalidOperationException(
                "Cannot use GetByteLength on this element type in a DuckDB vector. ");
        }
    }

    /// <summary>
    /// Copy out a sub-span of bytes from a blob, string, or bit string.
    /// </summary>
    /// <param name="rowIndex">
    /// The (row) index of the element that is a blob, string or bit string.
    /// </param>
    /// <param name="destination">
    /// Buffer where the bytes will be copied to.
    /// </param>
    /// <param name="totalBytes">
    /// The number of bytes that would be copied (given the same <paramref name="offset" />)
    /// if <paramref name="destination" /> is big enough.
    /// </param>
    /// <param name="offset">
    /// The offset, measured in byte units from the beginning of the blob, string or
    /// bit string, to start copying from.
    /// </param>
    /// <returns>
    /// The number of bytes written to the beginning of <paramref name="destination" />.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The element type of this DuckDB vector is not a blob, string or bit string.
    /// </exception>
    /// <remarks>
    /// <para>
    /// For a vector element that is a string, its byte content is the UTF-8 encoding of the string.
    /// </para>
    /// <para>
    /// For a vector element that is a bit string, the byte content is the little-endian encoding
    /// as accepted by <see cref="BitArray" />.
    /// (See also <see cref="DuckDbBitString.GetSegment" />.
    /// </para>
    /// <para>
    /// This method is primarily intended for implementing 
    /// <see cref="System.Data.IDataRecord.GetBytes(int, long, byte[], int, int)" />.
    /// </para>
    /// </remarks>
    public int GetBytes(int rowIndex, Span<byte> destination, out int totalBytes, int offset = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(offset, 0);
        var valueKind = ColumnInfo.ValueKind;
        int bytesWritten;

        if (valueKind == DuckDbValueKind.Blob ||
            valueKind == DuckDbValueKind.VarChar)
        {
            var reader = new DuckDbVectorRawReader<DuckDbBlob>(_vector);
            var blob = reader.GetItem(rowIndex);
            var source = blob.Span;
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(offset, source.Length);
            totalBytes = source.Length - offset;
            bytesWritten = Math.Min(destination.Length, totalBytes);
            source.Slice(offset, bytesWritten).CopyTo(destination);
        }
        else if (valueKind == DuckDbValueKind.Bit)
        {
            var reader = new DuckDbVectorRawReader<DuckDbBitString>(_vector);
            var bitString = reader.GetItem(rowIndex);

            int sourceByteLength = (bitString.Length + 7) / 8;
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(offset, sourceByteLength);
            totalBytes = sourceByteLength - offset;

            int totalBits = bitString.Length - 8 * offset;
            int bitsToWrite = Math.Min(destination.Length * 8, totalBits);
            bytesWritten = bitString.GetSegment(destination, 8 * offset, bitsToWrite);
        }
        else
        {
            throw new InvalidOperationException(
                "Cannot use GetBytes on this element type in a DuckDB vector. ");
        }

        GC.KeepAlive(this);
        return bytesWritten;
    }

    /// <summary>
    /// Copy out a sub-span of UTF-16 code units from a string.
    /// </summary>
    /// <param name="rowIndex">
    /// The (row) index of the element that is a blob, string or bit string.
    /// </param>
    /// <param name="destination">
    /// Buffer where the bytes will be copied to.
    /// </param>
    /// <param name="charOffset">
    /// The offset, measured in UTF-16 code units from the beginning of the string 
    /// to start copying from.
    /// </param>
    /// <returns>
    /// The number of UTF-16 code units written to the beginning of <paramref name="destination" />.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The element type of this DuckDB vector is not a string.
    /// </exception>
    /// <remarks>
    /// This method is primarily intended for implementing 
    /// <see cref="System.Data.IDataRecord.GetChars(int, long, char[], int, int)" />.
    /// </remarks>
    public int GetChars(int rowIndex, Span<char> destination, int charOffset = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(charOffset, 0);
        if (ColumnInfo.ValueKind != DuckDbValueKind.VarChar)
        {
            throw new InvalidOperationException(
                "Cannot use GetChars on a DuckDB vector whose element type is not VarChar (storing strings). ");
        }

        var reader = new DuckDbVectorRawReader<DuckDbString>(_vector);
        var sourceItem = reader.GetItem(rowIndex);
        var utf8Source = sourceItem.Utf8;

        if (destination.Length == 0)
            return 0;

        // Skip the specified number of UTF-16 codepoints at the beginning by linear scan,
        // which is the only approach possible.  We incrementally convert into a dummy buffer
        // (at some loss of efficiency) rather than re-implement a UTF-8 decoder ourselves.
        static int FindByteOffsetForCharOffset(ReadOnlySpan<byte> utf8Source, int charOffset, out char trailingSurrogate)
        {
            trailingSurrogate = '\0';

            if (charOffset <= 0)
                return 0;

            const int ChunkSize = 256;
            Span<char> dummyBuffer = stackalloc char[ChunkSize];
            int byteOffset = 0;
            int charsRemaining = charOffset;

            do
            {
                if (byteOffset >= utf8Source.Length)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(charOffset),
                        "Specified charOffset exceeds the length of the string. ");
                }

                var status = Utf8.ToUtf16(utf8Source[byteOffset..],
                                          dummyBuffer[0..Math.Min(ChunkSize, charsRemaining)],
                                          out int bytesSkipped, out int charsSkipped,
                                          replaceInvalidSequences: false, isFinalBlock: true);
                CheckUtf8Status(status);

                // Freak case where charOffset chops a UTF-16 surrogate pair in the middle.
                // Repeat the conversion for the surrogate pair, and then output the low surrogate
                // (trailing code unit), which the caller can set into the destination buffer.
                if (charsRemaining == 1 && charsSkipped == 0)
                {
                    Debug.Assert(bytesSkipped == 0);
                    trailingSurrogate = ReadSurrogatePair(utf8Source[byteOffset..]).Trailing;
                    bytesSkipped = 4;
                    charsSkipped = 2;
                }

                byteOffset += bytesSkipped;
                charsRemaining -= charsSkipped;
            } while (charsRemaining > 0);

            return byteOffset;
        }

        static (char Leading, char Trailing) ReadSurrogatePair(ReadOnlySpan<byte> utf8Source)
        {
            Span<char> surrogatePair = stackalloc char[2];
            var status = Utf8.ToUtf16(utf8Source, surrogatePair,
                                      out var bytesRead, out var charsWritten,
                                      replaceInvalidSequences: false, isFinalBlock: true);
            if (charsWritten != 2)
                status = OperationStatus.InvalidData;
            CheckUtf8Status(status);
            Debug.Assert(bytesRead == 4);
            Debug.Assert(Char.IsHighSurrogate(surrogatePair[0]) && Char.IsLowSurrogate(surrogatePair[1]));
            return (surrogatePair[0], surrogatePair[1]);
        }

        static void CheckUtf8Status(OperationStatus status)
        {
            if (status == OperationStatus.InvalidData)
                throw new InvalidOperationException("UTF-8 string is invalid and could not be converted to UTF-16. ");
        }

        // Tracks the byte position from start of UTF-8 string, for caching after this round of conversion.
        int byteOffset = 0;

        // Adjustment to charsWritten below for any unpaired surrogates written out.
        int surrogateCount = 0;

        if (charOffset > 0)
        {
            int oldCharOffset = 0;

            // Read cached positioning information from the last call.
            // Ignore it if it is not useful information.
            var cachedInfo = _cachedStringOffsetInfo.Value;
            if (cachedInfo.RowIndex == rowIndex && cachedInfo.CharOffset <= charOffset)
            {
                oldCharOffset = cachedInfo.CharOffset;
                byteOffset = cachedInfo.ByteOffset;
            }

            // After this statement completes, byteOffset corresponds to charOffset,
            // except in the case of a surrogate pair (see below).
            byteOffset += FindByteOffsetForCharOffset(utf8Source[byteOffset..], 
                                                      charOffset - oldCharOffset, 
                                                      out char trailingSurrogate);
            utf8Source = utf8Source[byteOffset..];

            // If charOffset points to the middle of a surrogate pair, then
            // FindByteOffsetForCharOffset skipped over the whole pair, i.e.
            // byteOffset actually corresponds to the character after the surrogate
            // pair.  "Back-fill" the trailing surrogate that was skipped,
            // and adjust charOffset by +1 to match.  (Note that this calculation is
            // consistent with the fact that the variable "charsRemaining" in that
            // function always ends up as -1 whenever this case occurs.)
            if (trailingSurrogate != '\0')
            {
                destination[0] = trailingSurrogate; // already ensured destination.Length >= 1
                destination = destination[1..];
                surrogateCount++;
                charOffset++;
            }
        }

        var status = Utf8.ToUtf16(utf8Source, destination, out int bytesRead, out int charsWritten,
                                  replaceInvalidSequences: false, isFinalBlock: true);
        CheckUtf8Status(status);

        // Fix up again if destination buffer is too short for the last surrogate pair.
        if (charsWritten == destination.Length - 1 && bytesRead < utf8Source.Length)
        {
            destination[^1] = ReadSurrogatePair(utf8Source[bytesRead..]).Leading;
            surrogateCount++;
        }

        // Cache positioning information if conversion has not ended for the current string.
        // Any unpaired surrogate coming from the "if" block immediately above shall not be counted.
        if (status == OperationStatus.DestinationTooSmall)
        {
            _cachedStringOffsetInfo.Value = (RowIndex: rowIndex,
                                             CharOffset: charOffset + charsWritten,
                                             ByteOffset: byteOffset + bytesRead);
        }

        return charsWritten + surrogateCount;
    }

    /// <summary>
    /// Last result of the search for the byte offset in <see cref="GetChars(int, Span{char}, int)" />.
    /// </summary>
    /// <remarks>
    /// <para>
    /// If the user calls <see cref="GetChars(int, Span{char}, int)" /> in a loop to read
    /// a single string in consecutive segments, this caching prevents the algorithm
    /// from incurring running time that is quadratic in the length of the string.
    /// </para>
    /// <para>
    /// <c>ByteOffset</c> is the position measured in bytes from the start of the UTF-8 string
    /// that corresponds to <c>CharOffset</c> which is the position measured in UTF-16 code units
    /// from the start of the UTF-16 string.
    /// </para>
    /// <para>
    /// Since UTF-8 to UTF-16 conversion cannot be restarted
    /// in the middle of the surrogate pair, there is an invariant that 
    /// <c>ByteOffset</c> and <c>CharOffset</c> will never
    /// be set to end up in the middle of a UTF-8 byte sequence / UTF-16 surrogate pair. 
    /// </para>
    /// </remarks>
    private Antitear<(int RowIndex, int CharOffset, int ByteOffset)> _cachedStringOffsetInfo;
    
    #endregion
}
