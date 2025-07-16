using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mallard.Tests;

public class DatabaseFixture : IDisposable
{
    public DuckDbConnection DbConnection { get; }

    public DatabaseFixture()
    {
        DbConnection = Program.MakeDbConnectionWithGeneratedData();
    }

    public void Dispose()
    {
        DbConnection.Dispose();
    }
}
