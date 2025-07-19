using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.Tests;

public class DatabaseFixture : IDisposable
{
    private readonly Lazy<DuckDbConnection> _dbConnection =
        new(Program.MakeDbConnectionWithGeneratedData);

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
