using System.IO;
using Mallard;

namespace Mallard.Tests;

internal static class DatabaseExtensions
{
    /// <summary>
    /// Consume chunks, but only count the number of rows in total.
    /// </summary>
    public static int DestructivelyCount(this DuckDbResult result)
        => result.ProcessAllChunks(
            false,
            (in reader, _) => reader.Length,
            accumulate: (a, b) => a + b, 
            seed: 0);

    /// <summary>
    /// Read a file of SQL statements and execute them against a connection. 
    /// </summary>
    public static void ExecuteSqlScript(this DuckDbConnection connection, string scriptFilePath)
    {
        var script = File.ReadAllText(Path.Combine(Program.TestDataDirectory, scriptFilePath));
        connection.Execute(script);
    }
}
