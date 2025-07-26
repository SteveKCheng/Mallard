using Mallard.C_API;
using System;

namespace Mallard;
using unsafe LogicalTypeImplFn = delegate*<void*, _duckdb_logical_type*>;

/// <summary>
/// Supplies context to factory methods for vector element converters,
/// i.e. those that construct <see cref="VectorElementConverter" />.
/// </summary>
/// <remarks>
/// <para>
/// Conversions for certain "complex" types require this context to be
/// carried out properly.  For simple types, the plain .NET structure 
/// <see cref="DuckDbColumnInfo" /> is sufficient; but for "complex" types 
/// need to analyze the "logical type" provided by DuckDB.  Such complex types
/// include enumerations and lists.
/// </para>
/// <para>
/// Since the latter is a native resource, scope needs to be controlled 
/// properly.  Hence this type is a "ref struct".  Even though it holds no 
/// managed references directly, we want to disallow any capturing of the
/// pointers (that are valid only locally) into GC objects.
/// </para>
/// <para>
/// Also, in the C API of DuckDB, "logical types" are represented
/// by handles that support only unique ownership, not shared ownership.  
/// That means, if the state object for a converter needs to capture the logical 
/// type, it must have its own copy of it.  So handles simply cannots
/// be just passed along (by value), but a function must be provided that creates
/// new (uniquely-owned) handles to the logical type.  That thus necessitates the
/// use of the low-level mechanism of function pointers in this 
/// </para>
/// </remarks>
internal unsafe readonly ref struct ConverterCreationContext
{
    /// <summary>
    /// Describes the column (type) that the converter is to work on.
    /// </summary>
    public DuckDbColumnInfo ColumnInfo { get; }

    /// <summary>
    /// Obtain a handle to the "logical type" object from the native DuckDB library.
    /// </summary>
    /// <returns>
    /// A freshly-created handle.  The caller is responsible for disposing it:
    /// use of <see cref="NativeLogicalTypeHolder" /> is suggested.
    /// </returns>
    internal _duckdb_logical_type* GetNativeLogicalType()
        => _logicalTypeImplFn(_logicalTypeImplState);

    /// <summary>
    /// Type-erased state for <see cref="_logicalTypeImplFn" />.
    /// </summary>
    /// <remarks>
    /// This may be a pointer to a native library object, or some ref struct
    /// on the stack, which is assumed to be in scope as long as this ref struct
    /// is in scope.  This pointer may be used to chain to the "parent" converter's
    /// context.
    /// </remarks>
    private readonly void* _logicalTypeImplState;

    /// <summary>
    /// A function which takes <see cref="_logicalTypeImplState" /> 
    /// and returns a newly-allocated handle for the native logical type
    /// for the column.
    /// </summary>
    private readonly LogicalTypeImplFn _logicalTypeImplFn;

    /// <summary>
    /// Create a new context.
    /// </summary>
    private ConverterCreationContext(DuckDbColumnInfo columnInfo, 
                                     void* logicalTypeImplState,
                                     LogicalTypeImplFn logicalTypeImplFn)
    {
        ColumnInfo = columnInfo;
        _logicalTypeImplState = logicalTypeImplState;
        _logicalTypeImplFn = logicalTypeImplFn;
    }

    /// <summary>
    /// Wrapper around private constructor that is slightly more type-safe.
    /// </summary>
    internal static ConverterCreationContext Create<T>(
        DuckDbColumnInfo columnInfo,
        T* logicalTypeImplState,
        delegate*<T*, _duckdb_logical_type*> logicalTypeImplFn)
        where T : unmanaged
    {
        return new(columnInfo, logicalTypeImplState, (LogicalTypeImplFn)logicalTypeImplFn);
    }

    /// <summary>
    /// Construct context when an actual vector is available (not just the abstract column).
    /// </summary>
    internal static ConverterCreationContext FromVector(in DuckDbVectorInfo vector)
    {
        static _duckdb_logical_type* logicalTypeImplFn(_duckdb_vector* nativeVector)
            => NativeMethods.duckdb_vector_get_column_type(nativeVector);

        return Create(vector.ColumnInfo, vector.NativeVector, &logicalTypeImplFn);
    }
}
