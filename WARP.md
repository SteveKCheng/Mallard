# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

Mallard is an alternative .NET bindings library for DuckDB, designed for high performance and memory safety. It prioritizes efficient column-oriented data processing with minimal GC allocations.

**Key Features:**
- Memory and thread-safe public API using `ref struct` patterns
- High-performance column-oriented data access
- Support for .NET 9+ with AOT compilation goals
- Functional API design over "enterprisey" object orientation

## Common Commands

### Building
```bash
# Build entire solution
dotnet build

# Build from IDE
# Open Mallard.slnx in Visual Studio or JetBrains Rider
```

### Testing
```bash
# Run tests via command line
cd Mallard.Tests/
dotnet run

# Run tests in IDE
# Use Test Explorer in Visual Studio/Rider
# In Rider: Enable "Testing Platform support" in settings
```

### Creating NuGet Packages
```bash
# Create package for .NET bindings
cd Mallard/
dotnet pack
# Output: out/package/release/Mallard.«version».nupkg

# Create runtime package for specific platform
cd Mallard.Runtime/
dotnet pack -p:RuntimeIdentifier=linux-x64
# Output: out/package/release/Mallard.Runtime.linux-x64.«version».nupkg
```

### Running Single Tests
```bash
# TUnit framework is used for testing
cd Mallard.Tests/
dotnet run -- --filter "TestName"
```

## Architecture Overview

### Project Structure
- **Mallard/**: Core library with DuckDB bindings
- **Mallard.Tests/**: Test suite using TUnit framework
- **Mallard.Runtime/**: Native DuckDB library packaging
- **Mallard.DataFrames/**: Microsoft.Data.Analysis integration
- **native/**: Directory for DuckDB native libraries

### Core Components

#### Connection Management
- `DuckDbDatabase`: Internal database handle with reference counting
- `DuckDbConnection`: Main connection class for executing queries
- Thread-safe connection pooling via reference counting

#### Query Execution
- `DuckDbResult`: High-performance result set handling
- `DuckDbChunkReader`: Column-oriented data reading
- `DuckDbStatement`: Prepared statements with parameter binding
- `DuckDbResultChunk`: Chunk-based data processing

#### Type System
- `DuckDbTypeMapping`: Maps DuckDB types to .NET types
- `DuckDbColumnInfo`: Column metadata and type information
- Extensive support for DuckDB types (integers, decimals, dates, UUIDs, lists, etc.)

#### ADO.NET Compatibility
- `DuckDbDataReader`: ADO.NET-compatible data reader
- `DuckDbCommand`: Command execution interface
- Located in `Ado/` directory

#### Memory Safety
- `HandleRefCount`: Reference counting for native resources
- `Barricade`: Thread safety enforcement
- `Antitear`: Prevents use-after-dispose scenarios

### Native Interop
- `NativeMethods`: P/Invoke declarations for DuckDB C API
- `CustomMarshalling`: Optimized string marshalling
- Located in `Interop/` directory

## Development Patterns

### Error Handling
- `DuckDbException` for database-related errors
- Status checking pattern: `DuckDbException.ThrowOnFailure(status, message)`

### Resource Management
- All native resources use `using` patterns or automatic disposal
- Reference counting prevents premature disposal of shared resources

### Performance Considerations
- Column-oriented access preferred over row-oriented
- Minimal boxing/unboxing through generic specialization
- `ref struct` usage for zero-copy data access

### Type Mapping
- Strong typing with compile-time type safety
- Nullable support via `System.Nullable<T>`
- Custom converters for complex types

## Key Requirements

- **Target Framework**: .NET 9+ (will migrate to .NET 10 LTS)
- **Language Features**: Uses C# 13+ features including field-backed properties
- **Unsafe Code**: Enabled for high-performance native interop
- **AOT Compatibility**: Goal but not fully achieved yet (reflection required for complex types)

## Testing Framework

- Uses **hybrid framework**: TUnit execution engine with xUnit v3 assertions
- **Why this unusual setup?**
  - xUnit v3 doesn't support AOT execution, but Mallard needs AOT testing
  - TUnit supports AOT but has verbose async-based assertion API
  - xUnit assertions work better with `ref struct` types (no async required)
  - Originally was pure xUnit v3 until AOT was enabled recently
- Test configuration in `Mallard.Tests.csproj`
- Coverage via Microsoft.Testing.Extensions.CodeCoverage
- AOT analysis enabled in test project

## Build System Notes

- MSBuild-based with custom targets
- Automatic DuckDB native library downloading via `DuckDbDownload.targets`
- Version management through `DuckDbVersion.props`
- GitVersion for automatic versioning
- Artifacts output to `out/` directory