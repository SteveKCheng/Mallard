using System;

namespace Mallard.Tests;

public sealed class DatabaseFixture : IDisposable
{
    private readonly Lazy<DuckDbConnection> _dbConnection =
        new(() =>
        {
            var c = new DuckDbConnection("");
            try
            {
                c.ExecuteNonQuery("INSTALL tpch");
                c.ExecuteNonQuery("LOAD tpch");
                c.ExecuteNonQuery("CALL dbgen(sf = 0.2)");
                return c;
            }
            catch
            {
                c.Dispose();
                throw;
            }
        });

    /// <summary>
    /// Singleton database connection populated with tables generated
    /// from the TPCH extension.
    /// </summary>
    /// <remarks>
    /// <para>
    /// (The contents of) this database connection should not be modified.
    /// </para>
    /// <para>
    /// Generating the data takes a short while so we only want to
    /// do it once.  Also, as of this writing, DuckDB has a crash
    /// bug when the TPCH extension is loaded from multiple threads
    /// (as can happen when tests are run in parallel).
    /// </para>
    /// </remarks>
    public DuckDbConnection DbConnection => _dbConnection.Value;

    public DatabaseFixture()
    {
    }

    public void Dispose()
    {
        if (_dbConnection.IsValueCreated)
            _dbConnection.Value.Dispose();
    }
}
