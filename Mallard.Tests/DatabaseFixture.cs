using System;
[assembly: AssemblyFixture(typeof(Mallard.Tests.DatabaseFixture))]

namespace Mallard.Tests;

public class DatabaseFixture : IDisposable
{
    private readonly Lazy<DuckDbConnection> _dbConnection =
        new(Program.MakeDbConnectionWithGeneratedData);

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
