using Mallard.Interop;
using System;

namespace Mallard;

/// <summary>
/// Provides type information on a structure type in DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// Some information may be retrieved on-demand from DuckDB.
/// Thus this class holds a native resource (handle to a DuckDB logical type)
/// and is disposable.
/// </para>
/// </remarks>
public unsafe sealed class DuckDbStructColumns : IResultColumns, IDisposable
{
    private HandleRefCount _refCount;
    private _duckdb_logical_type* _nativeType;
    
    internal DuckDbTypeMapping TypeMapping { get; }

    /// <inheritdoc cref="IResultColumns.ColumnCount" />
    public int ColumnCount { get; }

    internal VectorElementConverter GetColumnConverter(int columnIndex, Type? targetType)
    {
        var columnInfo = GetColumnInfo(columnIndex);

        using var _ = _refCount.EnterScope(this);
        var context = ConverterCreationContext.Indexed.FromStructType(columnInfo,
                                                                      _nativeType,
                                                                      columnIndex,
                                                                      TypeMapping,
                                                                      out var state);

        var converter = VectorElementConverter.CreateForType(targetType, in context);
        if (!converter.IsValid)
            DuckDbVectorInfo.ThrowForWrongParamType(columnInfo, targetType ?? typeof(object));
        return converter;
    }

    VectorElementConverter IResultColumns.GetColumnConverter(int columnIndex, Type? targetType)
        => GetColumnConverter(columnIndex, targetType);

    private void CheckColumnIndex(int columnIndex)
    {
        if ((uint)columnIndex >= (uint)ColumnCount)
        {
            throw new ArgumentOutOfRangeException(nameof(columnIndex),
                $"Column index {columnIndex} is out of range for this STRUCT type. " +
                $"This STRUCT has {ColumnCount} columns.");
        }
    }

    /// <inheritdoc cref="IResultColumns.GetColumnInfo(int)" />
    public DuckDbColumnInfo GetColumnInfo(int columnIndex)
    {
        CheckColumnIndex(columnIndex);

        // FIXME: Not cached currently.
        using var _ = _refCount.EnterScope(this);
        using var holder = new NativeLogicalTypeHolder(NativeMethods.duckdb_struct_type_child_type(_nativeType, columnIndex));
        return new DuckDbColumnInfo(holder.NativeHandle);
    }

    /// <inheritdoc cref="IResultColumns.GetColumnName" />
    public string GetColumnName(int columnIndex)
    {
        CheckColumnIndex(columnIndex);
        using var _ = _refCount.EnterScope(this);
        return NativeMethods.duckdb_struct_type_child_name(_nativeType, columnIndex);
    }

    /// <summary>
    /// Get the name of the structure type in the DuckDB database.
    /// </summary>
    /// <returns>
    /// The name of the STRUCT type, if it has been defined in DuckDB,
    /// or the empty string if it is anonymous.
    /// </returns>
    public string GetDataTypeName()
    {
        using var _ = _refCount.EnterScope(this);
        return NativeMethods.duckdb_logical_type_get_alias(_nativeType);
    }

    /// <summary>
    /// Construct from a native DuckDB logical type representing a structure type.
    /// </summary>
    /// <param name="nativeType">
    /// Non-null DuckDB logical type, which must be
    /// for a STRUCT type.  Ownership is transferred from the caller when an instance
    /// is successfully constructed.
    /// </param>
    /// <param name="typeMapping">
    /// Type mapping used in deriving converters for the DuckDB types of the struct's members.
    /// </param>
    internal DuckDbStructColumns(ref _duckdb_logical_type* nativeType, DuckDbTypeMapping typeMapping)
    {
        TypeMapping = typeMapping;

        _nativeType = nativeType;
        ColumnCount = (int)NativeMethods.duckdb_struct_type_child_count(nativeType);

        // Ownership transfer from the caller.
        nativeType = default;
    }

    internal static DuckDbStructColumns Create(ref readonly ConverterCreationContext context)
    {
        var nativeType = context.GetNativeLogicalType();
        try
        {
            return new DuckDbStructColumns(ref nativeType, context.TypeMapping);
        }
        catch
        {
            NativeMethods.duckdb_destroy_logical_type(ref nativeType);
            throw;
        }
    }

    private void DisposeImpl(bool disposing)
    {
        if (!_refCount.PrepareToDisposeOwner())
            return;

        NativeMethods.duckdb_destroy_logical_type(ref _nativeType);
    }

    /// <inheritdoc cref="IDisposable.Dispose" />
    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    ~DuckDbStructColumns()
    {
        DisposeImpl(disposing: false);
    }

    /// <summary>
    /// Get access to the native "logical type" object from DuckDB while taking
    /// shared ownership of this instance.
    /// </summary>
    /// <param name="scope">
    /// Holder that should be disposed once the caller is finished with using
    /// the native object.  The scope prevents the native object from being
    /// destroyed from a concurrent thread calling <see cref="Dispose" />,
    /// or from garbage collection on this instance.
    /// </param>
    /// <returns>
    /// The native "logical type" object.  It is not null when this method succeeds.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// This instance has already been disposed.
    /// </exception>
    internal _duckdb_logical_type* BorrowNativeLogicalType(out HandleRefCount.Scope scope)
    {
        scope = _refCount.EnterScope(this);
        return _nativeType;
    }
}
