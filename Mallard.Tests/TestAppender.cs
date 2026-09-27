using System;
using System.IO;
using System.Text;
using Mallard.Types;
using TUnit.Core;
using Xunit;

namespace Mallard.Tests;

/// <summary>
/// Unit tests for DuckDB appender functionality (<see cref="DuckDbAppender"/>),
/// verifying row insertion, column type mapping, default/null values, bulk data,
/// schema qualification, and slot safety guard rails.
/// </summary>
public class TestAppender
{
    /// <summary>
    /// Verifies basic appender workflow: creates a table with integer, string, double, boolean,
    /// and date columns; inserts multiple rows via <see cref="DuckDbAppender.Append"/> and
    /// <see cref="DuckDbAppender.FinishRow"/>; and verifies rows and column values using
    /// <see cref="DuckDbChunkReader"/>.
    /// </summary>
    [Test]
    public void BasicAppend()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery(@"
            CREATE TABLE people (
                id INTEGER,
                name VARCHAR,
                height DOUBLE,
                is_active BOOLEAN,
                birthdate DATE
            )");

        using (var appender = connection.CreateAppender("people"))
        {
            appender.Append().Set(1);
            appender.Append().Set("Alice");
            appender.Append().Set(165.5);
            appender.Append().Set(true);
            appender.Append().Set(new DuckDbDate(9143)); // 1995-01-14
            appender.FinishRow();

            appender.Append().Set(2);
            appender.Append().Set("Bob");
            appender.Append().Set(180.0);
            appender.Append().Set(false);
            appender.Append().Set(new DuckDbDate(10000));
            appender.FinishRow();
        }

        Assert.Equal(2, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM people"));

        using var result = connection.Execute("SELECT id, name, height, is_active, birthdate FROM people ORDER BY id");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(2, reader.Length);

            var idCol = reader.GetColumn<int>(0);
            var nameCol = reader.GetColumn<string>(1);
            var heightCol = reader.GetColumn<double>(2);
            var activeCol = reader.GetColumn<bool>(3);
            var dateCol = reader.GetColumn<DuckDbDate>(4);

            Assert.Equal(1, idCol.GetItem(0));
            Assert.Equal("Alice", nameCol.GetItem(0));
            Assert.Equal(165.5, heightCol.GetItem(0));
            Assert.True(activeCol.GetItem(0));
            Assert.Equal(new DuckDbDate(9143), dateCol.GetItem(0));

            Assert.Equal(2, idCol.GetItem(1));
            Assert.Equal("Bob", nameCol.GetItem(1));
            Assert.Equal(180.0, heightCol.GetItem(1));
            Assert.False(activeCol.GetItem(1));
            Assert.Equal(new DuckDbDate(10000), dateCol.GetItem(1));

            return true;
        });
    }

    /// <summary>
    /// Validates <see cref="DuckDbAppender.Slot.SetDefault"/> and <see cref="DuckDbValue.SetNull"/>
    /// against column definitions with DEFAULT expressions and nullable columns, asserting validity
    /// masks via <see cref="DuckDbVectorReader{T}.IsItemValid"/> and <see cref="DuckDbVectorReader{T}.GetItemOrDefault"/>.
    /// </summary>
    [Test]
    public void AppendDefaultAndNull()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery(@"
            CREATE TABLE items (
                id INTEGER,
                name VARCHAR DEFAULT 'default_item',
                count INTEGER DEFAULT 42,
                note VARCHAR
            )");

        using (var appender = connection.CreateAppender("items"))
        {
            // Row 1: using defaults and null
            appender.Append().Set(1);
            appender.Append().SetDefault();
            appender.Append().SetDefault();
            appender.Append().SetNull();
            appender.FinishRow();

            // Row 2: explicit values
            appender.Append().Set(2);
            appender.Append().Set("gadget");
            appender.Append().Set(10);
            appender.Append().Set("in stock");
            appender.FinishRow();
        }

        Assert.Equal(2, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM items"));

        using var result = connection.Execute("SELECT id, name, count, note FROM items ORDER BY id");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(2, reader.Length);

            var idCol = reader.GetColumn<int>(0);
            var nameCol = reader.GetColumn<string>(1);
            var countCol = reader.GetColumn<int>(2);
            var noteCol = reader.GetColumn<string>(3);

            Assert.Equal(1, idCol.GetItem(0));
            Assert.Equal("default_item", nameCol.GetItem(0));
            Assert.Equal(42, countCol.GetItem(0));
            Assert.False(noteCol.IsItemValid(0));
            Assert.Null(noteCol.GetItemOrDefault(0));

            Assert.Equal(2, idCol.GetItem(1));
            Assert.Equal("gadget", nameCol.GetItem(1));
            Assert.Equal(10, countCol.GetItem(1));
            Assert.True(noteCol.IsItemValid(1));
            Assert.Equal("in stock", noteCol.GetItem(1));

            return true;
        });
    }

    /// <summary>
    /// Exhaustively verifies native types mapped through <see cref="ISettableDuckDbValue"/>:
    /// signed and unsigned fixed-width integers (8, 16, 32, 64, 128-bit), floating point numbers
    /// (float, double), decimal (<see cref="DuckDbDecimal"/>), blobs (<see cref="DuckDbValue.SetBlob"/>),
    /// strings, dates (<see cref="DuckDbDate"/>), timestamps (<see cref="DuckDbTimestamp"/>), and intervals (<see cref="DuckDbInterval"/>).
    /// </summary>
    [Test]
    public void AppendDataTypes()
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
                c_dec DECIMAL(10, 2),
                c_str VARCHAR,
                c_blob BLOB,
                c_date DATE,
                c_ts TIMESTAMP,
                c_interval INTERVAL
            )");

        byte[] blobBytes = [0xDE, 0xAD, 0xBE, 0xEF];

        using (var appender = connection.CreateAppender("all_types"))
        {
            appender.Append().Set((sbyte)-8);
            appender.Append().Set((short)-16);
            appender.Append().Set(-32);
            appender.Append().Set(-64L);
            appender.Append().Set((Int128)(-128));
            appender.Append().Set((byte)8);
            appender.Append().Set((ushort)16);
            appender.Append().Set(32u);
            appender.Append().Set(64ul);
            appender.Append().Set((UInt128)128);
            appender.Append().Set(1.5f);
            appender.Append().Set(2.75);
            appender.Append().Set(123.45m);
            appender.Append().Set("Mallard");
            appender.Append().SetBlob(blobBytes);
            appender.Append().Set(new DuckDbDate(100));
            appender.Append().Set(new DuckDbTimestamp(1_000_000));
            appender.Append().Set(new DuckDbInterval { Months = 2, Days = 5, Microseconds = 1000 });
            appender.FinishRow();
        }

        Assert.Equal(1, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM all_types"));

        using var result = connection.Execute("SELECT * FROM all_types");
        result.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            Assert.Equal(1, reader.Length);

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
            Assert.Equal(123.45m, reader.GetColumn<decimal>(12).GetItem(0));
            Assert.Equal("Mallard", reader.GetColumn<string>(13).GetItem(0));
            Assert2.Equal(blobBytes, reader.GetColumn<byte[]>(14).GetItem(0));
            Assert.Equal(new DuckDbDate(100), reader.GetColumn<DuckDbDate>(15).GetItem(0));
            Assert.Equal(new DuckDbTimestamp(1_000_000), reader.GetColumn<DuckDbTimestamp>(16).GetItem(0));

            var interval = reader.GetColumn<DuckDbInterval>(17).GetItem(0);
            Assert.Equal(2, interval.Months);
            Assert.Equal(5, interval.Days);
            Assert.Equal(1000, interval.Microseconds);

            return true;
        });
    }

    /// <summary>
    /// Bulk insertion of 5,000 rows to ensure chunk transitions and buffer handling perform
    /// properly, followed by aggregate sum and count assertions.
    /// </summary>
    [Test]
    public void AppendBulkData()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE bulk_data (id INTEGER, val BIGINT)");

        const int rowCount = 5000;
        using (var appender = connection.CreateAppender("bulk_data"))
        {
            for (int i = 0; i < rowCount; i++)
            {
                appender.Append().Set(i);
                appender.Append().Set((long)i * 2);
                appender.FinishRow();
            }
        }

        Assert.Equal(rowCount, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM bulk_data"));
        long expectedSum = ((long)(rowCount - 1) * rowCount / 2) * 2;
        Assert.Equal(expectedSum, connection.ExecuteValue<long>("SELECT SUM(val)::BIGINT FROM bulk_data"));
    }

    /// <summary>
    /// Tests <see cref="DuckDbConnection.CreateAppender(string, string)"/> targeting non-default schemas.
    /// </summary>
    [Test]
    public void AppendWithExplicitSchema()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE SCHEMA test_schema");
        connection.ExecuteNonQuery("CREATE TABLE test_schema.numbers (num INTEGER)");

        using (var appender = connection.CreateAppender("test_schema", "numbers"))
        {
            appender.Append().Set(42);
            appender.FinishRow();
        }

        Assert.Equal(1, connection.ExecuteValue<int>("SELECT COUNT(*)::INTEGER FROM test_schema.numbers"));
        Assert.Equal(42, connection.ExecuteValue<int>("SELECT num FROM test_schema.numbers"));
    }

    /// <summary>
    /// Ensures attempting to create an appender for a non-existent table throws <see cref="DuckDbException"/>
    /// with <see cref="DuckDbErrorKind.Catalog"/> and descriptive native error message.
    /// </summary>
    [Test]
    public void NonExistentTableThrows()
    {
        using var connection = new DuckDbConnection("");
        var e = Assert.Throws<DuckDbException>(() => connection.CreateAppender("does_not_exist"));
        Assert.Equal(DuckDbErrorKind.Catalog, e.ErrorKind);
        Assert.Contains("does_not_exist", e.Message);
    }

    /// <summary>
    /// Ensures that calling <see cref="DuckDbAppender.FinishRow"/> before all columns have been
    /// appended throws a <see cref="DuckDbException"/> with descriptive error data.
    /// </summary>
    [Test]
    public void PrematureFinishRowThrows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE multi_col (a INTEGER, b VARCHAR, c DOUBLE)");

        using var appender = connection.CreateAppender("multi_col");
        appender.Append().Set(1);

        // Table has 3 columns, but only 1 value was appended before FinishRow
        var e = Assert.Throws<DuckDbException>(() => appender.FinishRow());
        Assert.NotEmpty(e.Message);
        Assert.Contains("EndRow", e.Message);
        Assert.Equal(DuckDbErrorKind.InvalidInput, e.ErrorKind);
    }

    /// <summary>
    /// Verifies that calling .Set(...) twice on the same <see cref="DuckDbAppender.Slot"/> instance
    /// triggers an <see cref="InvalidOperationException"/> due to sequence counter mismatch.
    /// </summary>
    [Test]
    public void SlotSafetyCannotReuseSlot()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER, b INTEGER)");

        using var appender = connection.CreateAppender("t");
        var slot = appender.Append();
        slot.Set(1);

        // Attempting to set again on the same slot must throw InvalidOperationException
        Assert.Throws<InvalidOperationException>(() => slot.Set(2));
    }

    /// <summary>
    /// Verifies that calling <see cref="DuckDbAppender.Append"/> multiple times and then attempting
    /// to write through an older, stale slot triggers an <see cref="InvalidOperationException"/>.
    /// </summary>
    [Test]
    public void SlotSafetyCannotUseStaleSlot()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER, b INTEGER)");

        using var appender = connection.CreateAppender("t");
        var slot1 = appender.Append();
        var slot2 = appender.Append();

        // Calling slot2.Set increments the sequence counter
        slot2.Set(10);

        // slot1 is now stale; setting it must throw InvalidOperationException
        Assert.Throws<InvalidOperationException>(() => slot1.Set(20));
    }

    /// <summary>
    /// Checks that invoking methods on a disposed appender throws <see cref="ObjectDisposedException"/>
    /// via <see cref="Barricade"/>.
    /// </summary>
    [Test]
    public void DisposedAppenderThrows()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER)");

        var appender = connection.CreateAppender("t");
        appender.Dispose();

        Assert.Throws<ObjectDisposedException>(() => appender.FinishRow());
    }
}
