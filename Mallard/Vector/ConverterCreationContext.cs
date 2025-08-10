using Mallard.C_API;
using System;
using System.Runtime.CompilerServices;

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
    /// Conventions for mapping certain types "by default".
    /// </summary>
    /// <remarks>
    /// This overrides <see cref="DuckDbTypeMapping.TypeMappingFlags" />.
    /// </remarks>
    public DuckDbTypeMappingFlags TypeMappingFlags { get; }

    /// <summary>
    /// Information on how to convert types as passed in by the user.
    /// </summary>
    public DuckDbTypeMapping TypeMapping { get; }

    /// <summary>
    /// Obtain a handle to the "logical type" object from the native DuckDB library.
    /// </summary>
    /// <returns>
    /// A freshly-created handle.  The caller is responsible for disposing it:
    /// use of <see cref="NativeLogicalTypeHolder" /> is suggested.
    /// </returns>
    /// <exception cref="DuckDbException">
    /// DuckDB returned a null handle.
    /// </exception>
    internal _duckdb_logical_type* GetNativeLogicalType()
    {
        var p = _logicalTypeImplFn(_logicalTypeImplState);
        if (p == null)
        {
            throw new DuckDbException(
                "Could not query the logical type from DuckDB. " +
                "There is either a bug (in Mallard or DuckDB), or memory was exhausted in obtaining a native resource. ");
        }

        return p;
    }

    /// <summary>
    /// Create an "enumeration dictionary" over the members of the enumeration.
    /// </summary>
    /// <returns>
    /// The numeration dictionary, which is typically used
    /// to construct converters from the enumeration type in DuckDB to a .NET representation. 
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The DuckDB column (in <see cref="ColumnInfo" /> is not typed as an enumeration.
    /// </exception>
    public DuckDbEnumDictionary CreateEnumDictionary()
    {
        if (ColumnInfo.ValueKind != DuckDbValueKind.Enum)
            throw new InvalidOperationException("Cannot get enumeration dictionary for a type that is not an enumeration. ");

        var nativeType = GetNativeLogicalType();
        if (nativeType == null)
            throw new DuckDbException("Could not query the logical type of a vector from DuckDB. ");

        try
        {
            return new DuckDbEnumDictionary(ref nativeType, (uint)ColumnInfo.ElementSize);
        }
        catch
        {
            NativeMethods.duckdb_destroy_logical_type(ref nativeType);
            throw;
        }
    }

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
                                     LogicalTypeImplFn logicalTypeImplFn,
                                     DuckDbTypeMapping typeMapping,
                                     DuckDbTypeMappingFlags flags)
    {
        ColumnInfo = columnInfo;
        TypeMappingFlags = flags;
        TypeMapping = typeMapping;

        _logicalTypeImplState = logicalTypeImplState;
        _logicalTypeImplFn = logicalTypeImplFn;
    }

    /// <summary>
    /// Wrapper around private constructor that is slightly more type-safe.
    /// </summary>
    internal static ConverterCreationContext Create<T>(
        DuckDbColumnInfo columnInfo,
        T* logicalTypeImplState,
        delegate*<T*, _duckdb_logical_type*> logicalTypeImplFn,
        DuckDbTypeMapping typeMapping,
        DuckDbTypeMappingFlags flags)
        where T : unmanaged, allows ref struct
    {
        return new(columnInfo, logicalTypeImplState, (LogicalTypeImplFn)logicalTypeImplFn, typeMapping, flags);
    }

    /// <summary>
    /// Construct context when an actual vector is available (not just the abstract column).
    /// </summary>
    internal static ConverterCreationContext FromVector(in DuckDbVectorInfo vector, 
                                                        DuckDbTypeMapping typeMapping,
                                                        DuckDbTypeMappingFlags flags = default)
    {
        static _duckdb_logical_type* logicalTypeImplFn(_duckdb_vector* nativeVector)
            => NativeMethods.duckdb_vector_get_column_type(nativeVector);

        return Create(vector.ColumnInfo, vector.NativeVector, &logicalTypeImplFn, typeMapping, flags);
    }

    /// <summary>
    /// State (to be created on the stack only) required for <see cref="FromColumn" />. 
    /// </summary>
    internal readonly ref struct ColumnDescriptor
    {
        /// <summary>
        /// Borrowed handle to DuckDB result object.
        /// </summary>
        internal readonly ref duckdb_result _nativeResult;

        /// <summary>
        /// Index of desired column to create a converter context for.
        /// </summary>
        internal readonly int _columnIndex;

        internal ColumnDescriptor(ref duckdb_result nativeResult, int columnIndex)
        {
            _nativeResult = ref nativeResult;
            _columnIndex = columnIndex;
        }
    }

    /// <summary>
    /// Construct context for a DuckDB column (without an actual vector).
    /// </summary>
    /// <param name="columnInfo">
    /// Basic information already gathered on the column.
    /// </param>
    /// <param name="target">
    /// Holds the native result object from DuckDB, and the target column index.
    /// This structure must be created by the caller, and the caller must keep it alive
    /// to it while the returned context is active, because the returned context
    /// takes a pointer to this argument.
    /// </param>
    internal static ConverterCreationContext FromColumn(in DuckDbColumnInfo columnInfo, 
                                                        ref ColumnDescriptor target, 
                                                        DuckDbTypeMapping typeMapping,
                                                        DuckDbTypeMappingFlags flags)
    {
        static _duckdb_logical_type* logicalTypeImplFn(void* p)
        {
            // Suppress warning:
            // "This takes the address of, gets the size of, or declares a pointer to a managed type"
            //
            // This is fine because ResultAndColumnIndex is a ref struct so it cannot ever move
            // (i.e. does not need pinning)
#pragma warning disable CS8500 
            var target = (ColumnDescriptor*)p;
#pragma warning restore CS8500
            return NativeMethods.duckdb_column_logical_type(ref target->_nativeResult, target->_columnIndex);
        }

        return new(columnInfo, Unsafe.AsPointer(ref target), &logicalTypeImplFn, typeMapping, flags);
    }

    internal bool ConvertDatesAsDateTime => (TypeMappingFlags & DuckDbTypeMappingFlags.DatesAsDateTime) != 0;
}
