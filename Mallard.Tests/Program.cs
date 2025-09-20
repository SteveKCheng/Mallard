using System;
using System.IO;
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
                        // According to https://learn.microsoft.com/en-us/dotnet/standard/native-interop/native-library-loading,
                        // NativeLibrary.Load does not adapt file names of the library for the OS conventions 
                        // if an absolute path is given, so we have to do so ourselves.
                        string fileName;
                        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                            fileName = $"{libraryName}.dll";
                        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                            fileName = $"lib{libraryName}.dylib";
                        else // assume Linux-like
                            fileName = $"lib{libraryName}.so";

                        var versionString = DuckDbConnection.OriginalNativeLibraryVersion;
                        var path = Path.Join(SolutionDirectory, 
                            "native", versionString, RuntimeInformation.RuntimeIdentifier, fileName);

                        DuckDbDllHandle = NativeLibrary.Load(path);
                    }

                    return DuckDbDllHandle;
                }

                return IntPtr.Zero;
            });
    }

    public static readonly string SolutionDirectory =
        Path.Join(System.AppContext.BaseDirectory, "..", "..", "..", "..");
    
    public static readonly string TestDataDirectory =
        Path.Join(SolutionDirectory, "testData");
}
