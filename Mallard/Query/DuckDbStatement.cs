using Mallard.C_API;
using System;
using System.Collections.Generic;

namespace Mallard;

/// <summary>
/// A prepared statement from a DuckDB database.
/// </summary>
/// <remarks>
/// <para>
/// Objects of this class may not be accessed from multiple threads simultaneously.
/// If that is attempted, exceptions will be thrown.
/// Binding parameters to values obviously mutates state which would require
/// callers to synchronize anyway.  Furthermore, the underlying object in the DuckDB
/// native library that implements execution of prepared statements is not thread-safe.
/// To execute the same prepared statement (possibly with different parameters) from
/// multiple threads, each thread must work with its own instance of this class.
/// </para>
/// </remarks>
public unsafe class DuckDbStatement : IDisposable
{
    private _duckdb_prepared_statement* _nativeStatement;
    private readonly int _numParams;
    private Barricade _barricade;

    #region Statement execution

    /// <summary>
    /// Execute the prepared statement and return the results (of the query).
    /// </summary>
    /// <returns>
    /// The results of the query execution.
    /// </returns>
    public DuckDbResult Execute()
        => Execute(DuckDbTypeMappingFlags.Default);

    private DuckDbResult Execute(DuckDbTypeMappingFlags typeMappingFlags)
    {
        duckdb_state status;
        duckdb_result nativeResult;

        using (var _ = _barricade.EnterScope(this))
        {
            // N.B. DuckDB captures the "client context" from the database connection
            // when NativeMethods.duckdb_prepare is called, and holds it with shared ownership.
            // Thus the connection object is not needed to execute the prepared statement,
            // (and the originating DuckDbConnection object does not have to be "locked").
            status = NativeMethods.duckdb_execute_prepared(_nativeStatement, out nativeResult);
        }

        return DuckDbResult.CreateFromQuery(status, ref nativeResult, typeMappingFlags);
    }

    /// <summary>
    /// Execute the prepared statement and return the results via an ADO.NET data reader.
    /// </summary>
    public DuckDbDataReader ExecuteReader()
        => new DuckDbDataReader(Execute(DuckDbTypeMappingFlags.DatesAsDateTime));

    /// <summary>
    /// Execute the prepared statement, and report only the number of rows changed.
    /// </summary>
    /// <returns>
    /// The number of rows changed by the execution of the statement.
    /// The result is -1 if the statement did not change any rows, or is otherwise
    /// a statement or query for which DuckDB does not report the number of rows changed.
    /// </returns>
    public long ExecuteNonQuery()
    {
        duckdb_result nativeResult;

        using (var _ = _barricade.EnterScope(this))
        {
            // Status can be ignored since any errors can be extracted from nativeResult
            NativeMethods.duckdb_execute_prepared(_nativeStatement, out nativeResult);
        }

        return DuckDbResult.TakeNumberOfChangedRows(ref nativeResult);
    }

    /// <summary>
    /// Execute the prepared statement, and return the first item in the results.
    /// </summary>
    /// <returns>
    /// The first row and cell of the results of the statement execution, if any.
    /// Null is returned if the statement does not produce any results.
    /// This method is typically for SQL statements that produce a single value.
    /// </returns>
    public object? ExecuteScalar()
        => ExecuteValue<object>();

    /// <summary>
    /// Execute the prepared statement, and return the first item in the results.
    /// </summary>
    /// <returns>
    /// <para>
    /// The first row and cell of the results of the statement execution, if any.
    /// This method is typically for SQL statements that produce a single value.
    /// </para>
    /// <para>
    /// The default value for <typeparamref name="T" /> is produced 
    /// when the SQL execution does not produce any results, unless
    /// the default value can be confused with a valid value, specifically
    /// when <typeparamref name="T" /> is a non-nullable value type.
    /// (This exception in behavior exists to avoid silently reading the
    /// wrong values.)  If <typeparamref name="T" /> is a reference type
    /// or nullable value type, the default value means "null".
    /// </para>
    /// </returns>
    public T? ExecuteValue<T>()
    {
        duckdb_state status;
        duckdb_result nativeResult;

        using (var _ = _barricade.EnterScope(this))
        {
            status = NativeMethods.duckdb_execute_prepared(_nativeStatement, out nativeResult);
        }

        return DuckDbResult.ExtractFirstCell<T>(status, ref nativeResult);
    }

    #endregion

    /// <summary>
    /// Wrap the native object for a prepared statement from DuckDB.
    /// </summary>
    /// <param name="nativeConn">
    /// The native connection object that the prepared statement is associated with.
    /// </param>
    /// <param name="sql">
    /// The SQL statement to prepare. 
    /// </param>
    internal DuckDbStatement(_duckdb_connection* nativeConn, string sql)
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
    /// The number of parameters in the prepared statement.
    /// </summary>
    /// <remarks>
    /// In this class, all indices of parameters are 1-based, i.e. the first parameter has index 1.
    /// This convention matches DuckDB's API and SQL syntax, where positional parameters
    /// are also 1-based.
    /// </remarks>
    public int ParameterCount => _numParams;

    /// <summary>
    /// Get the name of the parameter at the specified index.
    /// </summary>
    /// <param name="index">1-based index of the parameter. </param>
    /// <returns>The name of the parameter in the SQL statement.  If the parameter
    /// has no name, the empty string is returned. </returns>
    public string GetParameterName(int index)
    {
        ThrowIfParamIndexOutOfRange(index);

        using var _ = _barricade.EnterScope(this);
        return NativeMethods.duckdb_parameter_name(_nativeStatement, index);
    }

    public DuckDbValueKind GetParameterValueKind(int index)
    {
        ThrowIfParamIndexOutOfRange(index);

        using var _ = _barricade.EnterScope(this);
        return NativeMethods.duckdb_param_type(_nativeStatement, index);
    }

    public int GetParameterIndexForName(string name)
    {
        long index;
        duckdb_state status;

        using (var _ = _barricade.EnterScope(this))
        {
            status = NativeMethods.duckdb_bind_parameter_index(_nativeStatement, out index, name);
        }

        if (status != duckdb_state.DuckDBSuccess)
            throw new KeyNotFoundException($"Parameter with the given name was not found. Name: {name}");

        return (int)index;
    }

    #region Binding values to parameters
    
    /// <summary>
    /// Bind a positional parameter of the prepared statement to the specified value.
    /// </summary>
    /// <param name="index">1-based index of the parameter. </param>
    /// <param name="value">The value to set for the parameter. </param>
    /// <typeparam name="T">The .NET type of the value.
    /// It does not have to match the underlying DuckDB type; conversions
    /// will be performed as necessary.
    /// </typeparam>
    public void BindParameter<T>(int index, T value)
    {
        ThrowIfParamIndexOutOfRange(index);
        var nativeObject = DuckDbValue.CreateNativeObject(value);
        BindParameterInternal(index, ref nativeObject);
    }

    /// <summary>
    /// Bind a named parameter of the prepared statement to the specified value.
    /// </summary>
    /// <param name="name">The name of the parameter. </param>
    /// <param name="value">The value to set for the parameter. </param>
    /// <typeparam name="T">The .NET type of the value.
    /// It does not have to match the underlying DuckDB type; conversions
    /// will be performed as necessary.
    /// </typeparam>
    public void BindParameter<T>(string name, T value)
        => BindParameter<T>(GetParameterIndexForName(name), value); 

    private void BindParameterInternal(int index, ref _duckdb_value* nativeValue)
    {
        if (nativeValue == null)
            throw new DuckDbException("Failed to create object wrapping value. ");

        try
        {
            duckdb_state status;

            using (var _ = _barricade.EnterScope(this))
            {
                status = NativeMethods.duckdb_bind_value(_nativeStatement, index, nativeValue);
            }

            DuckDbException.ThrowOnFailure(status, "Could not bind specified value to parameter. ");
        }
        finally
        {
            NativeMethods.duckdb_destroy_value(ref nativeValue);
        }
    }
    
    #endregion

    #region Resource management

    private void DisposeImpl(bool disposing)
    {
        if (!_barricade.PrepareToDisposeOwner())
            return;

        NativeMethods.duckdb_destroy_prepare(ref _nativeStatement);
    }

    ~DuckDbStatement()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        DisposeImpl(disposing: false);
    }

    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    #endregion
}
