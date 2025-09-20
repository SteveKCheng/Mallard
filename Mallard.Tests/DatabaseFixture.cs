using System;
using System.IO;

namespace Mallard.Tests;

public sealed class DatabaseFixture : IDisposable
{
    private readonly Lazy<DuckDbConnection> _connectionWithTpchData =
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
    public DuckDbConnection ConnectionWithTpchData => _connectionWithTpchData.Value;

    private readonly Lazy<DuckDbConnection> _connectionWithNorthwind =
        new(() =>
        {
            var c = new DuckDbConnection("");
            try
            {
                c.ExecuteSqlScript("Northwind-Schema.sql");
                c.ExecuteSqlScript("Northwind-Data.sql");
                return c;
            }
            catch
            {
                c.Dispose();
                throw;
            }
        });
    
    /// <summary>
    /// Singleton database connection populated with tables and views
    /// from well-known Northwind example database from Microsoft.
    /// </summary>
    public DuckDbConnection ConnectionWithNorthwind => _connectionWithNorthwind.Value; 

    public DatabaseFixture()
    {
    }

    public void Dispose()
    {
        static void DisposeLazy<T>(Lazy<T> lazy) where T : IDisposable
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
        }
        
        DisposeLazy(_connectionWithTpchData);
        DisposeLazy(_connectionWithNorthwind);
    }
}
