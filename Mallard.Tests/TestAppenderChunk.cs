using System;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Mallard.Types;
using TUnit.Core;
using Xunit;

namespace Mallard.Tests;

/// <summary>
/// Unit tests for the chunk-writing appender API (<see cref="DuckDbAppender.AppendChunk{TState}"/>,
/// <see cref="DuckDbChunkWriter"/>, and <see cref="DuckDbVectorRawWriter{T}"/>): raw bulk insertion
/// by writing directly into the memory of a DuckDB data chunk.
/// </summary>
public class TestAppenderChunk
{
    /// <summary>
    /// Writes a single partial chunk of primitive columns through <see cref="DuckDbVectorMethods.AsSpan{T}(in DuckDbVectorRawWriter{T})"/>
    /// and verifies the rows round-trip back through a reader.
    /// </summary>
    [Test]
    public void AppendChunkBasic()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE points (id INTEGER, score DOUBLE)");

        int[] ids = [10, 20, 30];
        double[] scores = [1.5, 2.5, 3.5];

        using (var appender = connection.CreateTableAppender("points"))
        {
            var n = appender.AppendChunk((ids, scores),
                static (in DuckDbChunkWriter w, (int[] ids, double[] scores) data) =>
                {
                    var idCol = w.GetColumnRaw<int>(0).AsSpan();
                    var scoreCol = w.GetColumnRaw<double>(1).AsSpan();
                    for (var i = 0; i < data.ids.Length; i++)
                    {
                        idCol[i] = data.ids[i];
                        scoreCol[i] = data.scores[i];
                    }
                    return data.ids.Length;
                });

            Assert.Equal(3, n);
        }

        Assert.Equal(3, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM points"));

        using var result = connection.Execute("SELECT id, score FROM points ORDER BY id");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(3, reader.Length);
            var idCol = reader.GetColumn<int>(0);
            var scoreCol = reader.GetColumn<double>(1);
            for (var i = 0; i < 3; i++)
            {
                Assert.Equal(ids[i], idCol.GetItem(i));
                Assert.Equal(scores[i], scoreCol.GetItem(i));
            }
            return true;
        });
    }

    /// <summary>
    /// Also exercises the indexer-setter on <see cref="DuckDbVectorRawWriter{T}"/> rather than a span.
    /// </summary>
    [Test]
    public void AppendChunkViaSetItem()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER)");

        using (var appender = connection.CreateTableAppender("t"))
        {
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                var col = w.GetColumnRaw<int>(0);
                col[0] = 111;
                col.SetItem(1, 222);
                return 2;
            });
        }

        Assert.Equal(333, connection.ExecuteValue<int>("SELECT SUM(a)::INTEGER FROM t"));
    }

    /// <summary>
    /// Writes two rows of every supported primitive/fixed-width column type and verifies the values
    /// round-trip, confirming the raw writer's layout compatibility matches the raw reader's.
    /// </summary>
    [Test]
    public void AppendChunkAllPrimitiveTypes()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery(@"
            CREATE TABLE all_types (
                c_i8 TINYINT,
                c_i16 SMALLINT,
                c_i32 INTEGER,
                c_i64 BIGINT,
                c_i128 HUGEINT,
                c_u8 UTINYINT,
                c_u16 USMALLINT,
                c_u32 UINTEGER,
                c_u64 UBIGINT,
                c_u128 UHUGEINT,
                c_f32 FLOAT,
                c_f64 DOUBLE,
                c_bool BOOLEAN,
                c_date DATE,
                c_ts TIMESTAMP,
                c_interval INTERVAL
            )");

        using (var appender = connection.CreateTableAppender("all_types"))
        {
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                w.GetColumnRaw<sbyte>(0).AsSpan()[0] = -8;
                w.GetColumnRaw<sbyte>(0).AsSpan()[1] = 7;
                w.GetColumnRaw<short>(1).AsSpan()[0] = -16;
                w.GetColumnRaw<short>(1).AsSpan()[1] = 15;
                w.GetColumnRaw<int>(2).AsSpan()[0] = -32;
                w.GetColumnRaw<int>(2).AsSpan()[1] = 31;
                w.GetColumnRaw<long>(3).AsSpan()[0] = -64;
                w.GetColumnRaw<long>(3).AsSpan()[1] = 63;
                w.GetColumnRaw<Int128>(4).AsSpan()[0] = -128;
                w.GetColumnRaw<Int128>(4).AsSpan()[1] = 127;
                w.GetColumnRaw<byte>(5).AsSpan()[0] = 8;
                w.GetColumnRaw<byte>(5).AsSpan()[1] = 9;
                w.GetColumnRaw<ushort>(6).AsSpan()[0] = 16;
                w.GetColumnRaw<ushort>(6).AsSpan()[1] = 17;
                w.GetColumnRaw<uint>(7).AsSpan()[0] = 32;
                w.GetColumnRaw<uint>(7).AsSpan()[1] = 33;
                w.GetColumnRaw<ulong>(8).AsSpan()[0] = 64;
                w.GetColumnRaw<ulong>(8).AsSpan()[1] = 65;
                w.GetColumnRaw<UInt128>(9).AsSpan()[0] = 128;
                w.GetColumnRaw<UInt128>(9).AsSpan()[1] = 129;
                w.GetColumnRaw<float>(10).AsSpan()[0] = 1.5f;
                w.GetColumnRaw<float>(10).AsSpan()[1] = 2.5f;
                w.GetColumnRaw<double>(11).AsSpan()[0] = 2.75;
                w.GetColumnRaw<double>(11).AsSpan()[1] = 3.75;
                w.GetColumnRaw<byte>(12).AsSpan()[0] = 1;   // BOOLEAN stored as one byte
                w.GetColumnRaw<byte>(12).AsSpan()[1] = 0;
                w.GetColumnRaw<DuckDbDate>(13).AsSpan()[0] = new DuckDbDate(100);
                w.GetColumnRaw<DuckDbDate>(13).AsSpan()[1] = new DuckDbDate(200);
                w.GetColumnRaw<DuckDbTimestamp>(14).AsSpan()[0] = new DuckDbTimestamp(1_000_000);
                w.GetColumnRaw<DuckDbTimestamp>(14).AsSpan()[1] = new DuckDbTimestamp(2_000_000);
                w.GetColumnRaw<DuckDbInterval>(15).AsSpan()[0] = new DuckDbInterval { Months = 2, Days = 5, Microseconds = 1000 };
                w.GetColumnRaw<DuckDbInterval>(15).AsSpan()[1] = new DuckDbInterval { Months = 3, Days = 6, Microseconds = 2000 };
                return 2;
            });
        }

        using var result = connection.Execute("SELECT * FROM all_types ORDER BY c_i32");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(2, reader.Length);

            // Row 0 (c_i32 == -32)
            Assert.Equal((sbyte)-8, reader.GetColumn<sbyte>(0).GetItem(0));
            Assert.Equal((short)-16, reader.GetColumn<short>(1).GetItem(0));
            Assert.Equal(-32, reader.GetColumn<int>(2).GetItem(0));
            Assert.Equal(-64L, reader.GetColumn<long>(3).GetItem(0));
            Assert.Equal((Int128)(-128), reader.GetColumn<Int128>(4).GetItem(0));
            Assert.Equal((byte)8, reader.GetColumn<byte>(5).GetItem(0));
            Assert.Equal((ushort)16, reader.GetColumn<ushort>(6).GetItem(0));
            Assert.Equal(32u, reader.GetColumn<uint>(7).GetItem(0));
            Assert.Equal(64ul, reader.GetColumn<ulong>(8).GetItem(0));
            Assert.Equal((UInt128)128, reader.GetColumn<UInt128>(9).GetItem(0));
            Assert.Equal(1.5f, reader.GetColumn<float>(10).GetItem(0));
            Assert.Equal(2.75, reader.GetColumn<double>(11).GetItem(0));
            Assert.True(reader.GetColumn<bool>(12).GetItem(0));
            Assert.Equal(new DuckDbDate(100), reader.GetColumn<DuckDbDate>(13).GetItem(0));
            Assert.Equal(new DuckDbTimestamp(1_000_000), reader.GetColumn<DuckDbTimestamp>(14).GetItem(0));
            var iv0 = reader.GetColumn<DuckDbInterval>(15).GetItem(0);
            Assert.Equal(2, iv0.Months);
            Assert.Equal(5, iv0.Days);
            Assert.Equal(1000, iv0.Microseconds);

            // Row 1 (c_i32 == 31)
            Assert.Equal((sbyte)7, reader.GetColumn<sbyte>(0).GetItem(1));
            Assert.Equal((short)15, reader.GetColumn<short>(1).GetItem(1));
            Assert.Equal(31, reader.GetColumn<int>(2).GetItem(1));
            Assert.Equal(63L, reader.GetColumn<long>(3).GetItem(1));
            Assert.Equal((Int128)127, reader.GetColumn<Int128>(4).GetItem(1));
            Assert.Equal((byte)9, reader.GetColumn<byte>(5).GetItem(1));
            Assert.Equal((ushort)17, reader.GetColumn<ushort>(6).GetItem(1));
            Assert.Equal(33u, reader.GetColumn<uint>(7).GetItem(1));
            Assert.Equal(65ul, reader.GetColumn<ulong>(8).GetItem(1));
            Assert.Equal((UInt128)129, reader.GetColumn<UInt128>(9).GetItem(1));
            Assert.Equal(2.5f, reader.GetColumn<float>(10).GetItem(1));
            Assert.Equal(3.75, reader.GetColumn<double>(11).GetItem(1));
            Assert.False(reader.GetColumn<bool>(12).GetItem(1));
            Assert.Equal(new DuckDbDate(200), reader.GetColumn<DuckDbDate>(13).GetItem(1));
            Assert.Equal(new DuckDbTimestamp(2_000_000), reader.GetColumn<DuckDbTimestamp>(14).GetItem(1));
            var iv1 = reader.GetColumn<DuckDbInterval>(15).GetItem(1);
            Assert.Equal(3, iv1.Months);
            Assert.Equal(6, iv1.Days);
            Assert.Equal(2000, iv1.Microseconds);

            return true;
        });
    }

    /// <summary>
    /// Appends more rows than fit in a single chunk, driving the loop with the row count returned by
    /// <see cref="DuckDbAppender.AppendChunk{TState}"/> and so exercising chunk reuse (reset) across calls.
    /// </summary>
    [Test]
    public void AppendChunkBulkAcrossMultipleChunks()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE bulk_chunk (id INTEGER, val BIGINT)");

        const int rowCount = 5000;
        using (var appender = connection.CreateTableAppender("bulk_chunk"))
        {
            var written = 0;
            while (written < rowCount)
            {
                written += appender.AppendChunk(written, (in DuckDbChunkWriter w, int off) =>
                {
                    var n = Math.Min(rowCount - off, w.Capacity);
                    var ids = w.GetColumnRaw<int>(0).AsSpan();
                    var vals = w.GetColumnRaw<long>(1).AsSpan();
                    for (var i = 0; i < n; i++)
                    {
                        ids[i] = off + i;
                        vals[i] = (long)(off + i) * 2;
                    }
                    return n;
                });
            }
        }

        Assert.Equal(rowCount, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM bulk_chunk"));
        var expectedSum = ((long)(rowCount - 1) * rowCount / 2) * 2;
        Assert.Equal(expectedSum, connection.ExecuteValue<long>("SELECT SUM(val)::BIGINT FROM bulk_chunk"));
    }

    /// <summary>
    /// Writing a chunk of zero rows is a no-op that inserts nothing.
    /// </summary>
    [Test]
    public void AppendChunkZeroRows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER)");

        using (var appender = connection.CreateTableAppender("t"))
        {
            var n = appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) => 0);
            Assert.Equal(0, n);
        }

        Assert.Equal(0, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM t"));
    }

    /// <summary>
    /// Requesting a column with a .NET type that does not match the column's storage type throws.
    /// </summary>
    [Test]
    public void AppendChunkWrongColumnTypeThrows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER)");

        using var appender = connection.CreateTableAppender("t");
        Assert.Throws<ArgumentException>(() =>
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                w.GetColumnRaw<long>(0);   // column is INTEGER, not BIGINT
                return 0;
            }));
    }

    /// <summary>
    /// Returning a row count greater than the chunk capacity throws <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    [Test]
    public void AppendChunkRowCountTooLargeThrows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER)");

        using var appender = connection.CreateTableAppender("t");
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) => w.Capacity + 1));
    }

    /// <summary>
    /// Returning a negative row count throws <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    [Test]
    public void AppendChunkNegativeRowCountThrows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER)");

        using var appender = connection.CreateTableAppender("t");
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) => -1));
    }

    /// <summary>
    /// Requesting a column index that does not exist throws <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    [Test]
    public void AppendChunkColumnIndexOutOfRangeThrows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER, b INTEGER)");

        using var appender = connection.CreateTableAppender("t");
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                w.GetColumnRaw<int>(5);
                return 0;
            }));
    }

    /// <summary>
    /// Marks some elements invalid with <see cref="DuckDbVectorRawWriter{T}.SetInvalid"/> and verifies
    /// they read back as SQL NULL while the others keep their values.
    /// </summary>
    [Test]
    public void AppendChunkWithNulls()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (id INTEGER, val DOUBLE)");
        
        // A test sequence of indices where the indices are to be invalid.
        // The first 24 members of the "The Lazy Caterer's Sequence":
        //   1, 2, 4, 7, 11, 16, 22, ..., 211, 232, 254
        var invalids = new bool[256];
        for (int n = 0; n < 23; ++n)
            invalids[(n * n + n + 2) / 2] = true;

        using (var appender = connection.CreateTableAppender("t"))
        {
            appender.AppendChunk(0, (in DuckDbChunkWriter w, int _) =>
            {
                var id = w.GetColumnRaw<int>(0).AsSpan();
                var valCol = w.GetColumnRaw<double>(1);
                var val = valCol.AsSpan();

                var valMask = valCol.ValidityMask;
                
                for (var i = 0; i < 256; i++)
                {
                    id[i] = i;
                    val[i] = i * 10.0;

                    if (invalids[i])
                    {
                        // For i < 32, try setting the validity mask directly.
                        if (i < 32)
                            valMask[0] &= ~(1ul << i);
                        else
                            valCol.SetInvalid(i);
                    }
                }

                return 256;
            });
        }

        Assert.Equal(256, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM t"));
        Assert.Equal(invalids.Count(false), connection.ExecuteValue<int>("SELECT COUNT(val)::INTEGER FROM t"));

        using var result = connection.Execute("SELECT id, val FROM t ORDER BY id");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(256, reader.Length);
            var idCol = reader.GetColumn<int>(0);
            var valCol = reader.GetColumn<double>(1);

            for (int i = 0; i < 256; i++)
            {
                Assert.Equal(i, idCol.GetItem(i));
                if (invalids[i])
                    Assert.False(valCol.IsItemValid(i));
                else
                    Assert.Equal(i * 10.0, valCol.GetItem(i));
            }

            return true;
        });
    }

    /// <summary>
    /// Confirms that the validity mask is cleared between chunk-writes when the cached chunk is reused:
    /// a NULL written at an index in the first chunk must not leak into the same index of the next chunk.
    /// </summary>
    [Test]
    public void AppendChunkNullsClearedOnReuse()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (id INTEGER, val INTEGER)");

        using (var appender = connection.CreateTableAppender("t"))
        {
            // First chunk: two rows, row 0's value is NULL.
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                var id = w.GetColumnRaw<int>(0).AsSpan();
                var valCol = w.GetColumnRaw<int>(1);
                var val = valCol.AsSpan();
                id[0] = 0; val[0] = 999;
                id[1] = 1; val[1] = 111;
                valCol.SetInvalid(0);
                return 2;
            });

            // Second chunk reuses the same native chunk: row 0 must come back valid (not NULL),
            // proving duckdb_data_chunk_reset cleared the validity mask.
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                var id = w.GetColumnRaw<int>(0).AsSpan();
                var val = w.GetColumnRaw<int>(1).AsSpan();
                id[0] = 2; val[0] = 222;
                return 1;
            });
        }

        // Only the first chunk's row 0 is NULL; the reused chunk's row 0 (id == 2) is valid.
        Assert.Equal(3, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM t"));
        Assert.Equal(2, connection.ExecuteValue<int>("SELECT COUNT(val)::INTEGER FROM t"));
        Assert.Equal(222, connection.ExecuteValue<int>("SELECT val::INTEGER FROM t WHERE id = 2"));
    }

    /// <summary>
    /// Verify that multiple shallow copies of the column/writer structure share the same validity mask,
    /// and that setting an element removes any previously flagging of the same element as invalid.  
    /// </summary>
    [Test]
    public void CheckValidityMaskConsistency()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (id INTEGER, val DOUBLE)");
        const int rowCount = 400; 

        using (var appender = connection.CreateTableAppender("t"))
        {
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                var idCol = w.GetColumnRaw<int>(0);
                var valCol = w.GetColumnRaw<double>(1);

                // "Shallow copy" the column ref structs and ensure the cached validity masks are the same  
                var idColCopy = idCol;
                var valColCopy = w.GetColumnRaw<double>(1);

                var idMask = idColCopy.ValidityMask;
                var valMask = valColCopy.ValidityMask;

                Assert.True(Unsafe.AreSame(ref MemoryMarshal.GetReference(idMask), 
                    ref MemoryMarshal.GetReference(idCol.ValidityMask)));
                
                // Deliberately set everything as invalid first
                for (int i = 0; i < rowCount; i++)
                {
                    idCol.SetInvalid(i);
                    valCol.SetInvalid(i);
                }
                
                Assert.True(Unsafe.AreSame(ref MemoryMarshal.GetReference(valMask),
                    ref MemoryMarshal.GetReference(valCol.ValidityMask)));

                // Now set values for odd-numbered rows in the "id" column,
                // and for even-numbered rows in the "val" column 
                for (int i = 0; i < rowCount; i += 2)
                {
                    idCol.SetItem(i + 1, i + 1);
                    valCol.SetItem(i, i * 10.0);
                }
                
                // Check validity mask by reading its memory (through span)
                for (int i = 0; i < rowCount; i++)
                {
                    bool isIdValid = (idMask[i / 64] & (1ul << (i % 64))) != 0;
                    bool isValValid = (valMask[i / 64] & (1ul << (i % 64))) != 0;
                    Assert.Equal((i % 2) != 0, isIdValid);
                    Assert.Equal((i % 2) == 0, isValValid);
                }
                
                // Check validity masks have the right length!
                Assert.Equal((idCol.AsSpan().Length + 63) / 64, idMask.Length);
                Assert.Equal((valCol.AsSpan().Length + 63) / 64, valMask.Length);

                return rowCount;
            });
        }

        static int SumArithmeticSequence(int start, int increment, int count)
            => count * start + increment * (count * (count - 1)) / 2;
            
        // Check summed up values of columns are as expected,
        // confirming that DuckDB is seeing the exact same elements being valid or invalid
        Assert.Equal(SumArithmeticSequence(1, 2, rowCount / 2), connection.ExecuteValue<int>("SELECT SUM(id)::INTEGER FROM t"));
        Assert.Equal(SumArithmeticSequence(0, 20, rowCount / 2), connection.ExecuteValue<int>("SELECT SUM(val)::INTEGER FROM t"));
        
        // Check length of validity mask of result.
        using var result = connection.Execute("SELECT * FROM t");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            var idCol = reader.GetColumnRaw<int>(0);
            var valCol = reader.GetColumnRaw<double>(1);
            var idMask = idCol.ValidityMask;
            var valMask = valCol.ValidityMask;
            Assert.Equal((reader.Length + 63) / 64, idMask.Length);
            Assert.Equal((reader.Length + 63) / 64, valMask.Length);
            return true;
        });
    }

    /// <summary>
    /// A raw writer cannot set variable-length (VARCHAR/BLOB) elements, since DuckDB must own their
    /// memory; attempting to do so must throw rather than corrupt the vector.
    /// </summary>
    [Test]
    public void AppendChunkRejectsVariableLengthColumns()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (s VARCHAR, b BLOB)");

        using var appender = connection.CreateTableAppender("t");

        Assert.Throws<NotSupportedException>(() =>
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                w.GetColumnRaw<DuckDbString>(0).SetItem(0, default);
                return 0;
            }));

        Assert.Throws<NotSupportedException>(() =>
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                w.GetColumnRaw<DuckDbBlob>(1).SetItem(0, default);
                return 0;
            }));
    }

    /// <summary>
    /// Writes VARCHAR and BLOB elements through the <see cref="DuckDbVectorMethods.Set(in DuckDbVectorRawWriter{DuckDbString}, int, string)"/>,
    /// <c>SetStringUtf16</c>, <c>SetStringUtf8</c>, and <c>SetBlob</c> helpers — covering a short string,
    /// a multi-byte UTF-8 string, a string too long for the stack buffer (exercising the heap fallback),
    /// an empty blob, and a value later marked NULL — then verifies the round-trip.
    /// </summary>
    [Test]
    public void AppendChunkSetStringAndBlob()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (id INTEGER, name VARCHAR, data BLOB)");

        // Longer than Utf8StringConverterState.SuggestedBufferSize (0x200) so the marshaller must
        // allocate a temporary heap buffer.
        var longString = new string('x', 1000);
        var utf8Hello = System.Text.Encoding.UTF8.GetBytes("héllo");   // "héllo": é is 2 UTF-8 bytes
        byte[] blob0 = [0xDE, 0xAD, 0xBE, 0xEF];
        var blobBig = new byte[1000];
        Array.Fill(blobBig, (byte)0x5A);

        using (var appender = connection.CreateTableAppender("t"))
        {
            appender.AppendChunk(0, (in DuckDbChunkWriter w, int _) =>
            {
                var id = w.GetColumnRaw<int>(0).AsSpan();
                var name = w.GetColumnRaw<DuckDbString>(1);
                var data = w.GetColumnRaw<DuckDbBlob>(2);

                id[0] = 0; name.Set(0, "Alice");                   data.SetBlob(0, blob0);
                id[1] = 1; name.SetStringUtf8(1, utf8Hello);       data.SetBlob(1, ReadOnlySpan<byte>.Empty);
                id[2] = 2; name.SetStringUtf16(2, longString.AsSpan()); data.SetBlob(2, blobBig);
                id[3] = 3; name.Set(3, "overwritten"); name.SetInvalid(3); data.SetBlob(3, [0x01]);
                return 4;
            });
        }

        Assert.Equal(4, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM t"));
        Assert.Equal(3, connection.ExecuteValue<int>("SELECT COUNT(name)::INTEGER FROM t"));

        using var result = connection.Execute("SELECT id, name, data FROM t ORDER BY id");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(4, reader.Length);
            var nameCol = reader.GetColumn<string>(1);
            var dataCol = reader.GetColumn<byte[]>(2);

            Assert.Equal("Alice", nameCol.GetItem(0));
            Assert2.Equal(blob0, dataCol.GetItem(0));

            Assert.Equal("héllo", nameCol.GetItem(1));
            Assert.Empty(dataCol.GetItem(1));

            Assert.Equal(longString, nameCol.GetItem(2));
            Assert2.Equal(blobBig, dataCol.GetItem(2));

            Assert.False(nameCol.IsItemValid(3));
            Assert2.Equal(new byte[] { 0x01 }, dataCol.GetItem(3));

            return true;
        });
    }

    [Test]
    public void FlushDataEarly()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY, val DOUBLE)");

        using (var appender1 = connection.CreateTableAppender("t"))
        {
            // No constraint violation while data is being buffered
            for (int i = 0; i < 4000; ++i)
            {
                appender1.Append().SetNull();
                appender1.Append().Set(i * 10.0);
                appender1.FinishRow();
            }
        
            // Constraint violation detected as soon as data is flushed
            Assert.Throws<DuckDbException>(() => appender1.Flush(), 
                e => e.ErrorKind == DuckDbErrorKind.Constraint 
                    ? null : "Did not get a constraint violation on flushing bad data");
        }
    
        using (var appender2 = connection.CreateTableAppender("t"))
        {
            for (int i = 0; i < 4000; ++i)
            {
                appender2.Append().Set(i);
                appender2.Append().Set(i * 10.0);

                // Don't finish the last row
                if (i != 3999)
                    appender2.FinishRow();
            }
            
            Assert.Throws<DuckDbException>(() => appender2.Flush(),
                e => e.ErrorKind == DuckDbErrorKind.InvalidInput
                    ? null : "Did not get a 'invalid input' complaint on flushing an incomplete row");
        }
    }

    /// <summary>
    /// Mixing row-wise appends with <see cref="DuckDbAppender.AppendChunk{TState}"/> must preserve the
    /// overall append order in the table.  <see cref="DuckDbAppender.AppendChunk{TState}"/> implicitly
    /// flushes the preceding row-wise rows first; without that, DuckDB would commit the chunk's rows
    /// ahead of the still-buffered row-wise rows.  Physical insertion order is observed through the
    /// <c>rowid</c> pseudo-column.
    /// </summary>
    [Test]
    public void AppendChunkImplicitlyFlushesPrecedingRowWiseAppends()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (seq INTEGER)");

        using (var appender = connection.CreateTableAppender("t"))
        {
            // seq 0, 1 — row-wise
            appender.Append().Set(0); appender.FinishRow();
            appender.Append().Set(1); appender.FinishRow();

            // seq 2, 3, 4 — chunk
            appender.AppendChunk(2, static (in DuckDbChunkWriter w, int baseSeq) =>
            {
                var seq = w.GetColumnRaw<int>(0).AsSpan();
                for (int i = 0; i < 3; i++)
                    seq[i] = baseSeq + i;
                return 3;
            });

            // seq 5, 6 — row-wise again
            appender.Append().Set(5); appender.FinishRow();
            appender.Append().Set(6); appender.FinishRow();

            // seq 7, 8 — chunk again
            appender.AppendChunk(7, static (in DuckDbChunkWriter w, int baseSeq) =>
            {
                var seq = w.GetColumnRaw<int>(0).AsSpan();
                for (int i = 0; i < 2; i++)
                    seq[i] = baseSeq + i;
                return 2;
            });
        }

        Assert.Equal(9, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM t"));

        // rowid reflects physical insertion order; seq must come back strictly ascending 0..8, which
        // holds only because AppendChunk flushed the preceding row-wise rows (in order) before
        // inserting the chunk.  If it did not, the chunk rows would have been committed first.
        using var result = connection.Execute("SELECT seq FROM t ORDER BY rowid");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(9, reader.Length);
            var seqCol = reader.GetColumn<int>(0);
            for (int i = 0; i < reader.Length; i++)
                Assert.Equal(i, seqCol.GetItem(i));
            return true;
        });
    }

    /// <summary>
    /// A chunk append after a partially-written row triggers the implicit flush, which surfaces the
    /// incomplete row as <see cref="DuckDbErrorKind.InvalidInput"/>.  (The chunk-writing function is
    /// never invoked, because the flush happens first.)
    /// </summary>
    [Test]
    public void AppendChunkAfterPartialRowThrows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER, b INTEGER)");

        using var appender = connection.CreateTableAppender("t");
        appender.Append().Set(1);   // only one of two columns appended; row left unfinished

        var e = Assert.Throws<DuckDbException>(() =>
            appender.AppendChunk(0, static (in DuckDbChunkWriter w, int _) =>
            {
                w.GetColumnRaw<int>(0).AsSpan()[0] = 10;
                w.GetColumnRaw<int>(1).AsSpan()[0] = 20;
                return 1;
            }));
        Assert.Equal(DuckDbErrorKind.InvalidInput, e.ErrorKind);
    }
}
