using Mallard.C_API;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Mallard;

/// <summary>
/// Prepared statement.
/// </summary>
public unsafe class DuckDbCommand
{
    private _duckdb_prepared_statement* _nativeStatement;
    private readonly int _numParams;
    private readonly Lock _mutex = new();
    private bool _isDisposed;

    internal DuckDbCommand(_duckdb_connection* nativeConn, string sql)
    {
        var status = NativeMethods.duckdb_prepare(nativeConn, sql, out var nativeStatement);
        try
        {
            if (status == duckdb_state.DuckDBError)
            {
                var errorMessage = NativeMethods.duckdb_prepare_error(nativeStatement);
                throw new DuckDbException(errorMessage);
            }

            _numParams = (int)NativeMethods.duckdb_nparams(nativeStatement);
        }
        catch
        {
            NativeMethods.duckdb_destroy_prepare(ref nativeStatement);
            throw;
        }

        _nativeStatement = nativeStatement;
    }

    private void ThrowIfParamIndexOutOfRange(int index)
    {
        if (unchecked((uint)index - 1u >= (uint)_numParams))
            throw new IndexOutOfRangeException("Index of parameter is out of range. ");
    }

    /// <summary>
    /// Get the name of the parameter at the specified index.
    /// </summary>
    /// <param name="index">
    /// 1-based index of the parameter.
    /// </param>
    /// <returns>The name of the parameter in the SQL statement.  If the parameter
    /// has no name, the empty string is returned. </returns>
    public string GetParameterName(int index)
    {
        ThrowIfParamIndexOutOfRange(index);

        lock (_mutex)
        {
            ThrowIfDisposed();
            return NativeMethods.duckdb_parameter_name(_nativeStatement, index);
        }
    }

    public DuckDbBasicType GetParameterBasicType(int index)
    {
        ThrowIfParamIndexOutOfRange(index);

        lock (_mutex)
        {
            ThrowIfDisposed();
            return NativeMethods.duckdb_param_type(_nativeStatement, index);
        }
    }

    public int GetParameterIndexForName(string name)
    {
        long index;
        duckdb_state status;
        lock (_mutex)
        {
            ThrowIfDisposed();
            status = NativeMethods.duckdb_bind_parameter_index(_nativeStatement, out index, name);
        }
        if (status != duckdb_state.DuckDBSuccess)
            throw new KeyNotFoundException($"Parameter with the given name was not found. Name: {name}");
        return (int)index;
    }

    public void BindParameter<T>(int index, T value)
    {
        ThrowIfParamIndexOutOfRange(index);

        var _nativeObject = DuckDbValue.CreateNativeObject(value);
        if (_nativeObject == null)
            throw new DuckDbException("Failed to create object wrapping value. ");

        try
        {
            duckdb_state status;
            lock (_mutex)
            {
                status = NativeMethods.duckdb_bind_value(_nativeStatement, index, _nativeObject);
            }

            DuckDbException.ThrowOnFailure(status, "Could not bind specified value to parameter. ");
        }
        finally
        {
            NativeMethods.duckdb_destroy_value(ref _nativeObject);
        }
    }

    #region Destruction

    private void DisposeImpl(bool disposing)
    {
        lock (_mutex)
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            NativeMethods.duckdb_destroy_prepare(ref _nativeStatement);
        }
    }

    ~DuckDbCommand()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        DisposeImpl(disposing: false);
    }

    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    private void ThrowIfDisposed()
    {
        if (_isDisposed)
            throw new ObjectDisposedException("Cannot operate on this object after it has been disposed. ");
    }

    #endregion
}
