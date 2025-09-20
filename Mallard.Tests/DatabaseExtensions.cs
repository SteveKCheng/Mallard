using System.IO;
using Mallard;

namespace Mallard.Tests;

internal static class DatabaseExtensions
{
    public static int DestructivelyCount(this DuckDbResult result)
    {
        bool hasChunk;
        int totalRows = 0;
        do
        {
            hasChunk = result.ProcessNextChunk(false, (in DuckDbChunkReader reader, bool _) => reader.Length, out var length);
            totalRows += length;
        } while (hasChunk);
        return totalRows;
    }

    public static void ExecuteSqlScript(this DuckDbConnection connection, string scriptFilePath)
    {
        var script = File.ReadAllText(Path.Combine(Program.TestDataDirectory, scriptFilePath));
        connection.Execute(script);
    }
}
