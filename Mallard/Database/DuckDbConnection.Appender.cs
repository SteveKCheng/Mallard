using System;

namespace Mallard;

public partial class DuckDbConnection
{
    #region Appenders

    /// <summary>
    /// Prepare to insert data rows into a database table using DuckDB's "appender" functionality. 
    /// </summary>
    /// <param name="catalog">
    /// Names the catalog (database) that contains the table to append to, or null for the default catalog.
    /// The default catalog is the primary database file initially opened by this connection,
    /// or whichever database that was selected via a <c>USE</c> statement.
    /// </param>
    /// <param name="schema">
    /// Names the schema, within the named catalog, that contains the table to append to, or null for the default schema.
    /// </param>
    /// <param name="table">
    /// Names the table to append to, within the given database and schema.
    /// </param>
    /// <returns>
    /// The "appender" object to insert rows into the selected database table.
    /// </returns>
    /// <exception cref="DuckDbException">
    /// DuckDB failed to create to create the "appender" object.
    /// </exception>
    public unsafe DuckDbAppender CreateAppender(string? catalog, string? schema, string table)
    {
        using var _ = _refCount.EnterScope(this);
        return new DuckDbAppender(_nativeConn, catalog, schema, table);
    }

    /// <summary>
    /// Prepare to insert data rows into a database table using DuckDB's "appender" functionality. 
    /// </summary>
    /// <param name="table">
    /// Names the table to append to, within the default database and its default schema.
    /// </param>
    /// <returns>
    /// The "appender" object to insert rows into the selected database table.
    /// </returns>
    /// <exception cref="DuckDbException">
    /// DuckDB failed to create to create the "appender" object.
    /// </exception>
    public DuckDbAppender CreateAppender(string table) => CreateAppender(null, null, table);
    
    /// <summary>
    /// Prepare to insert data rows into a database table using DuckDB's "appender" functionality. 
    /// </summary>
    /// <param name="schema">
    /// Names the schema, within the default database, that contains the table to append to, or null for the default schema.
    /// </param>
    /// <param name="table">
    /// Names the table to append to, within the default database and the given schema.
    /// </param>
    /// <returns>
    /// The "appender" object to insert rows into the selected database table.
    /// </returns>
    /// <exception cref="DuckDbException">
    /// DuckDB failed to create to create the "appender" object.
    /// </exception>
    public DuckDbAppender CreateAppender(string schema, string table) => CreateAppender(null, schema, table);

    /// <summary>
    /// Prepare to insert data rows by streaming them through an arbitrary SQL statement, using
    /// DuckDB's query-based "appender" functionality.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike <see cref="CreateAppender(string)" />, which is limited to plain <c>INSERT</c> into a
    /// single table, the appended rows are exposed to <paramref name="query" /> as a virtual relation
    /// named <c>appended_data</c>, with columns <c>col1</c>, <c>col2</c>, ... (in declaration order).
    /// The query may therefore reference that relation to perform conflict resolution, merges, or
    /// filtered/transformed inserts, for example:
    /// <code>
    /// INSERT INTO people SELECT col1, col2 FROM appended_data WHERE col2 IS NOT NULL
    /// </code>
    /// </para>
    /// <para>
    /// Because the column types of <c>appended_data</c> cannot be inferred from the query, they must
    /// be supplied explicitly through <paramref name="columnTypes" />.  In this initial implementation
    /// only primitive .NET types are supported (as mapped by
    /// <see cref="DuckDbComplexTypeInfo.MapToNativeLogicalType" />); other types throw
    /// <see cref="NotSupportedException" />.
    /// </para>
    /// <para>
    /// Values are appended exactly as for a plain appender: call <see cref="DuckDbAppender.Append" />
    /// once per column in declaration order, then <see cref="DuckDbAppender.FinishRow" />.
    /// </para>
    /// </remarks>
    /// <param name="query">
    /// The SQL statement to execute against the appended rows.  Can be an <c>INSERT</c>, <c>DELETE</c>,
    /// <c>UPDATE</c>, or <c>MERGE INTO</c> statement that reads from the <c>appended_data</c> relation.
    /// </param>
    /// <param name="columnTypes">
    /// The .NET types of the columns of the <c>appended_data</c> relation, in declaration order.
    /// </param>
    /// <returns>
    /// The "appender" object to stream rows through <paramref name="query" />.
    /// </returns>
    /// <exception cref="NotSupportedException">
    /// One of the given column types is not a supported primitive type.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// DuckDB failed to create the query-based "appender" object, e.g. because <paramref name="query" />
    /// could not be parsed or bound.
    /// </exception>
    public unsafe DuckDbAppender CreateQueryAppender(string query, ReadOnlySpan<Type> columnTypes)
    {
        using var _ = _refCount.EnterScope(this);
        return new DuckDbAppender(_nativeConn, query, columnTypes);
    }

    #endregion
}
