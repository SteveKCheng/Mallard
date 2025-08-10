using Mallard.C_API;
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

    /// <inheritdoc cref="IResultColumns.ColumnCount" />
    public int ColumnCount { get; }

    VectorElementConverter IResultColumns.GetColumnConverter(int columnIndex, Type? targetType)
    {
        throw new NotImplementedException();
    }

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
    DuckDbColumnInfo IResultColumns.GetColumnInfo(int columnIndex)
    {
        CheckColumnIndex(columnIndex);

        // Not cached currently.
        using var _ = _refCount.EnterScope(this);
        using var holder = new NativeLogicalTypeHolder(NativeMethods.duckdb_struct_type_child_type(_nativeType, columnIndex));
        return new DuckDbColumnInfo(holder.NativeHandle);
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
    internal DuckDbStructColumns(ref _duckdb_logical_type* nativeType)
    {
        _nativeType = nativeType;
        ColumnCount = (int)NativeMethods.duckdb_struct_type_child_count(nativeType);

        // Ownership transfer from the caller.
        nativeType = default;
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
}
