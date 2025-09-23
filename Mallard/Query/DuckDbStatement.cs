using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Mallard.Interop;
using Mallard.Ado;

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
/// <para>
/// The SQL statement(s) that have been prepared behind this object are immutable.
/// Unlike the ADO.NET interface <see cref="System.Data.IDbCommand" />, you must
/// obtain new instances of class to represent different SQL statement(s).  Instances
/// are obtained via <see cref="DuckDbConnection.PrepareStatement" />.
/// </para>
/// </remarks>
public unsafe partial class DuckDbStatement : IDisposable
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
    /// <exception cref="ObjectDisposedException">
    /// This statement has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The statement failed to execute due to unbound parameters, constraint violations,
    /// or other database errors.
    /// </exception>
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
    /// <returns>
    /// An ADO.NET-compatible data reader for accessing the query results.
    /// </returns>
    /// <exception cref="ObjectDisposedException">
    /// This statement has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The statement failed to execute due to unbound parameters, constraint violations,
    /// or other database errors.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This statement has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The statement failed to execute due to unbound parameters, constraint violations,
    /// or other database errors.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This statement has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The statement failed to execute due to unbound parameters, constraint violations,
    /// or other database errors.
    /// </exception>
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
    /// <exception cref="ObjectDisposedException">
    /// This statement has been disposed.
    /// </exception>
    /// <exception cref="DuckDbException">
    /// The statement failed to execute due to unbound parameters, constraint violations,
    /// or other database errors.
    /// </exception>
    /// <exception cref="InvalidCastException">
    /// The first cell value cannot be converted to the type <typeparamref name="T" />.
    /// </exception>
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

    #region Binding values to parameters

    /// <summary>
    /// Clear all bindings of values to parameters in the prepared statement.
    /// </summary>
    public void ClearBindings()
    {
        using var _ = _barricade.EnterScope(this);
        var status = NativeMethods.duckdb_clear_bindings(_nativeStatement);
        DuckDbException.ThrowOnFailure(
            status, 
            "Failed to clear bindings of values to parameters in the prepared statement. ");
    }

    /// <summary>
    /// The collection of formal parameters in a prepared statement from DuckDB.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This collection is essentially an ordered list of the parameters.
    /// (It does not implement <see cref="IReadOnlyList{T}" /> only
    /// because DuckDB's parameters are defined to be numbered starting from 1, not 0.)
    /// </para>
    /// <para>
    /// Parameters may also be looked up by the name they have been defined
    /// under in the SQL prepared statement.
    /// </para>
    /// </remarks>
    public readonly struct ParametersCollection : IReadOnlyCollection<Parameter>
    {
        private readonly DuckDbStatement _parent;

        /// <inheritdoc />
        public IEnumerator<Parameter> GetEnumerator()
        {
            for (int i = 1; i <= Count; ++i)
                yield return this[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Get the number of parameters available to bind in the DuckDB statement.
        /// </summary>
        /// <value>
        /// The total number of parameters.
        /// </value>
        public int Count => _parent.ParameterCount;

        /// <summary>
        /// Get one of the parameters, by positional index.
        /// </summary>
        /// <param name="index">
        /// <para>
        /// 1-based index of the parameter.
        /// </para>
        /// <para>
        /// Currently, DuckDB does not support mixing named and positional parameters,
        /// so if positional parameters are being used, this index
        /// should match up exactly with the ordinal of the parameter in the SQL statement,
        /// e.g. <c>$1</c> maps to index 1.
        /// </para>
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="index" /> is less than 1 or greater than <see cref="Count" />.
        /// </exception>
        public Parameter this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _parent.ThrowIfParamIndexOutOfRange(index);
                return new Parameter(_parent, index);
            }
        }

        /// <summary>
        /// Get one of the parameters, by name. 
        /// </summary>
        /// <param name="name">The name of the parameter.
        /// For positional parameters in the SQL statement, the name is the decimal
        /// representation of the ordinal (in ASCII digits, no leading zeros).
        /// </param>
        /// <exception cref="ArgumentException">
        /// There is no parameter with the given name from the prepared statement.
        /// </exception>
        /// <exception cref="ObjectDisposedException">
        /// The prepared statement for this parameter collection has already been
        /// disposed.  (Looking up the name requires querying the native
        /// prepared statement object from DuckDB.)
        /// </exception>
        public Parameter this[string name]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var index = _parent.GetParameterIndexForName(name, throwIfNotFound: true);
                return new Parameter(_parent, index);
            }
        }

        /// <summary>
        /// Get the index of the parameter with the given name.
        /// </summary>
        /// <param name="name">
        /// The name of the parameter in the SQL statement.
        /// </param>
        /// <returns>
        /// The 1-based index of the named parameter, suitable for indexing into
        /// this collection.  If no parameter with the given name exists,
        /// -1 is returned.
        /// </returns>
        /// <exception cref="ObjectDisposedException">
        /// The prepared statement for this parameter collection has already been
        /// disposed.  (Looking up the name requires querying the native
        /// prepared statement object from DuckDB.)
        /// </exception>
        public int GetIndexForName(string name) 
            => _parent.GetParameterIndexForName(name, throwIfNotFound: false);
        
        internal ParametersCollection(DuckDbStatement parent)
        {
            _parent = parent;
        }
    }

    /// <summary>
    /// The collection of formal parameters in this prepared statement.
    /// </summary>
    public ParametersCollection Parameters => new ParametersCollection(this);

    #endregion
    
    #region Methods for implementing Parameter and ParameterCollection
    
    private void ThrowIfParamIndexOutOfRange(int index, [CallerArgumentExpression(nameof(index))] string? paramName = null)
    {
        if (unchecked((uint)index - 1u) >= (uint)_numParams)
            throw new ArgumentOutOfRangeException(paramName, "The index of the SQL parameter is out of range. ");
    }

    /// <summary>
    /// Binds a value of a parameter.
    /// </summary>
    private void BindParameter(int index, ref _duckdb_value* nativeValue)
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

            ThrowOnBindFailure(status);
        }
        finally
        {
            NativeMethods.duckdb_destroy_value(ref nativeValue);
        }
    }

    private void ThrowOnBindFailure(duckdb_state status)
        => DuckDbException.ThrowOnFailure(status, "Could not bind specified value to parameter. ");
    
    /// <summary>
    /// Get the name of the parameter at the specified index.
    /// </summary>
    private string GetParameterName(int index)
    {
        ThrowIfParamIndexOutOfRange(index);

        using var _ = _barricade.EnterScope(this);
        return NativeMethods.duckdb_parameter_name(_nativeStatement, index);
    }

    /// <summary>
    /// Get the DuckDB type of the parameter at the specified index.
    /// </summary>
    private DuckDbValueKind GetParameterValueKind(int index)
    {
        ThrowIfParamIndexOutOfRange(index);

        using var _ = _barricade.EnterScope(this);
        return NativeMethods.duckdb_param_type(_nativeStatement, index);
    }

    /// <summary>
    /// Get the index for a named parameter.
    /// </summary>
    private int GetParameterIndexForName(string name, bool throwIfNotFound)
    {
        long index;
        duckdb_state status;

        using (var _ = _barricade.EnterScope(this))
        {
            status = NativeMethods.duckdb_bind_parameter_index(_nativeStatement, out index, name);
        }

        if (status == duckdb_state.DuckDBSuccess)
            return (int)index;
        
        if (throwIfNotFound)
            throw new ArgumentException($"Parameter with the given name was not found. Name: {name}");

        return -1;
    }

    /// <summary>
    /// The number of parameters in the prepared statement.
    /// </summary>
    private int ParameterCount => _numParams;

    #endregion

    #region Resource management

    private void DisposeImpl(bool disposing)
    {
        if (!_barricade.PrepareToDisposeOwner())
            return;

        NativeMethods.duckdb_destroy_prepare(ref _nativeStatement);
    }

    /// <summary>
    /// Destructor which will dispose this object if it has yet been already.
    /// </summary>
    ~DuckDbStatement()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        DisposeImpl(disposing: false);
    }

    /// <summary>
    /// Disposes this object along with resources allocated in the native DuckDB library for it. 
    /// </summary>
    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    #endregion
}
