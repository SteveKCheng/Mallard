﻿using System;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard.Tests;

internal static class Program
{
    private static IntPtr DuckDbDllHandle;

    // Set up custom path to load the native library for DuckDB.
    // There is a trick to inject the paths to the libraries into the
    // "deps.json" file at compile-time
    // (see https://github.com/asmichi/DotNetInjectNativeFileDepsHack/)
    // but it requires more work than writing this bit of code.  
    [ModuleInitializer]

    internal static void Initialize()
    {
        NativeLibrary.SetDllImportResolver(typeof(DuckDbConnection).Assembly,
            (libraryName, _, _) =>
            {
                if (libraryName == "duckdb")
                {
                    if (DuckDbDllHandle == IntPtr.Zero)
                    {
                        var path = Path.Join(SolutionDirectory, "native", RuntimeInformation.RuntimeIdentifier, libraryName);
                        DuckDbDllHandle = NativeLibrary.Load(path);
                    }

                    return DuckDbDllHandle;
                }

                return IntPtr.Zero;
            });
    }

    public static readonly string SolutionDirectory =
        Path.Join(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "..", "..", "..", "..");
    
    public static readonly string TestDataDirectory =
        Path.Join(SolutionDirectory, "testData");

    internal static DuckDbConnection MakeDbConnectionWithGeneratedData()
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
    }

    internal static int DestructiveGetNumberOfResults(this DuckDbResult result)
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
}
