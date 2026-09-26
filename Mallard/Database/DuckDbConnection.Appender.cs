namespace Mallard;

public partial class DuckDbConnection
{
    #region Appenders

    /// <summary>
    /// Prepare to insert data rows into a database table using DuckDB's "appender" functionality. 
    /// </summary>
    /// <param name="catalog">
    /// Names the catalog (database) that contains the table to append to, or null for the default catalog.
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
    
    #endregion
}
