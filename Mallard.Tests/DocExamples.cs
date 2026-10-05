using System;
using Mallard.Types;
using TUnit.Core;
using Xunit;

namespace Mallard.Tests;

/// <summary>
/// Worked examples that are embedded into the generated documentation.
/// </summary>
/// <remarks>
/// <para>
/// Every <c>#region</c> in this file is pulled verbatim into the docs at
/// build time by DocFX, via <c>[!code-csharp[](...#RegionName)]</c> in the
/// Markdown under <c>docfx/</c>, or via <c>&lt;example&gt;</c> in an XML doc
/// comment. Because these are ordinary TUnit tests, an example that stops
/// compiling or stops passing breaks the build rather than silently rotting
/// on the website.
/// </para>
/// <para>
/// Consequences for anyone editing this file:
/// </para>
/// <list type="bullet">
///   <item>
///     Renaming or deleting a region silently empties the corresponding block
///     in the docs — DocFX does not warn. Run <c>docfx/check-snippets.sh</c>
///     (also wired into CI) after any change here.
///   </item>
///   <item>
///     Keep the code inside a region short and free of test scaffolding. The
///     assertions are deliberately left in: they document the expected result
///     and prove the snippet is real.
///   </item>
///   <item>
///     Prefer in-memory connections (<c>new DuckDbConnection("")</c>) so each
///     example stands alone and a reader can paste and run it.
///   </item>
/// </list>
/// </remarks>
public class DocExamples
{
    /// <summary>
    /// Open an in-memory database, create and populate a table, read one value back.
    /// </summary>
    [Test]
    public void QuickStart()
    {
        #region QuickStart
        // An empty path opens a purely in-memory database.
        using var connection = new DuckDbConnection("");

        connection.ExecuteNonQuery(
            "CREATE TABLE trades (symbol VARCHAR, quantity INTEGER, price DOUBLE)");
        connection.ExecuteNonQuery(
            """
            INSERT INTO trades VALUES
                ('QQQ', 100, 412.75),
                ('SPY', 250, 538.10),
                ('QQQ',  75, 414.00)
            """);

        var quantity = connection.ExecuteValue<int>(
            "SELECT SUM(quantity)::INTEGER FROM trades WHERE symbol = 'QQQ'");

        Assert.Equal(175, quantity);
        #endregion
    }

    /// <summary>
    /// Bind parameters by name on a prepared statement, and re-execute it.
    /// </summary>
    [Test]
    public void NamedParameters()
    {
        #region NamedParameters
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery(
            "CREATE TABLE trades (symbol VARCHAR, quantity INTEGER, price DOUBLE)");
        connection.ExecuteNonQuery(
            """
            INSERT INTO trades VALUES
                ('QQQ', 100, 412.75),
                ('SPY', 250, 538.10),
                ('QQQ',  75, 414.00)
            """);

        using var statement = connection.PrepareStatement(
            "SELECT COUNT(*)::INTEGER FROM trades WHERE symbol = $symbol AND price >= $floor");

        // Values are written without boxing, through a strongly-typed setter.
        statement.Parameters["symbol"].Set("QQQ");
        statement.Parameters["floor"].Set(413.0);
        Assert.Equal(1, statement.ExecuteValue<int>());

        // The same statement can be re-executed with different values.
        statement.Parameters["floor"].Set(400.0);
        Assert.Equal(2, statement.ExecuteValue<int>());
        #endregion
    }

    /// <summary>
    /// Read a result column-at-a-time, which is how DuckDB natively stores data.
    /// </summary>
    [Test]
    public void ReadColumns()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery(
            "CREATE TABLE trades (symbol VARCHAR, quantity INTEGER, price DOUBLE)");
        connection.ExecuteNonQuery(
            """
            INSERT INTO trades VALUES
                ('QQQ', 100, 412.75),
                ('SPY', 250, 538.10),
                ('QQQ',  75, 414.00)
            """);

        #region ReadColumns
        using var result = connection.Execute(
            "SELECT symbol, quantity FROM trades ORDER BY quantity");

        // Results arrive as "chunks" of up to a few thousand rows. The type of
        // each column is checked once per chunk, not once per value read.
        // To carry a value out, close over a local, as recommended by the
        // documentation for ProcessAllChunks.
        string? firstSymbol = null;

        result.ProcessAllChunks(
            state: false,
            function: (in DuckDbChunkReader reader, bool _) =>
            {
                var symbol = reader.GetColumn<string>(0);
                var quantity = reader.GetColumn<int>(1);

                Assert.Equal(3, reader.Length);
                Assert.Equal(75, quantity.GetItem(0));

                firstSymbol ??= symbol.GetItem(0);
                return true;
            });

        Assert.Equal("QQQ", firstSymbol);
        #endregion
    }

    /// <summary>
    /// Read a column of primitives directly out of DuckDB's own memory,
    /// with no copying and no GC allocation.
    /// </summary>
    [Test]
    public void ZeroCopyRead()
    {
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery(
            "CREATE TABLE readings AS SELECT i::DOUBLE * 1.5 AS value FROM range(1000) t(i)");

        #region ZeroCopyRead
        using var result = connection.Execute("SELECT value FROM readings");

        var total = result.ProcessAllChunks(
            state: false,
            function: (in DuckDbChunkReader reader, bool _) =>
            {
                // The span points straight at DuckDB's native buffer for this
                // chunk. Nothing is copied, nothing is boxed, and no GC object
                // is allocated for the values themselves.
                ReadOnlySpan<double> values = reader.GetColumnRaw<double>(0).AsSpan();

                var sum = 0.0;
                foreach (var value in values)
                    sum += value;
                return sum;
            },
            accumulate: static (a, b) => a + b,
            seed: 0.0);

        // 1.5 * (0 + 1 + ... + 999)
        Assert.Equal(1.5 * 999 * 1000 / 2, total);
        #endregion
    }

    /// <summary>
    /// Insert many rows efficiently through a DuckDB appender.
    /// </summary>
    [Test]
    public void BulkAppend()
    {
        #region BulkAppend
        using var connection = new DuckDbConnection("");
        connection.ExecuteNonQuery(
            "CREATE TABLE people (id INTEGER, name VARCHAR, birthdate DATE)");

        using (var appender = connection.CreateTableAppender("people"))
        {
            appender.Append().Set(1);
            appender.Append().Set("Alice");
            appender.Append().Set(new DuckDbDate(9143));   // 1995-01-14
            appender.FinishRow();

            appender.Append().Set(2);
            appender.Append().Set("Bob");
            appender.Append().Set(new DuckDbDate(10000));  // 1997-05-19
            appender.FinishRow();
        }   // rows are flushed when the appender is disposed

        Assert.Equal(2, connection.ExecuteValue<int>(
            "SELECT COUNT(*)::INTEGER FROM people"));
        #endregion
    }
}
