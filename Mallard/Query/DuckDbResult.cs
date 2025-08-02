using Mallard.C_API;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Mallard;

/// <summary>
/// Grants access to the results of a SQL execution by DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// The results are always presented in chunks by DuckDB.  
/// Each chunk can be requested in succession (allowing streaming queries).
/// </para>
/// <para>
/// Since retrieving a chunk advances the internal state of this result object,
/// i.e. mutating that state, methods in this class that do that may not be
/// called from multiple threads simultaneously.  Any attempt to do so will cause exceptions.
/// </para>
/// </remarks>
public unsafe sealed class DuckDbResult : IResultColumns, IDisposable
{
    #region Resource management

    private HandleRefCount _refCount;
    private duckdb_result _nativeResult;

    private void DisposeImpl(bool disposing)
    {
        if (!_refCount.PrepareToDisposeOwner())
            return;

        NativeMethods.duckdb_destroy_result(ref _nativeResult);
    }

    ~DuckDbResult()
    {
        DisposeImpl(disposing: false);
    }

    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    #endregion

    #region Construction

    private DuckDbResult(ref duckdb_result nativeResult)
    {
        _nativeResult = nativeResult;

        var columnCount = (int)NativeMethods.duckdb_column_count(ref _nativeResult);

        _columns = new Column[columnCount];
        for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex)
        {
            _columns[columnIndex] = new Column
            {
                Info = new DuckDbColumnInfo(ref _nativeResult, columnIndex)
            };
        }

        // Ownership transfer
        nativeResult = default;
    }

    /// <summary>
    /// Wrap the native result from DuckDB, and handle errors. 
    /// </summary>
    /// <remarks>
    /// This code is common to prepared and non-prepared queries.
    /// </remarks>
    /// <param name="status">
    /// Return status from executing a query in DuckDB. 
    /// </param>
    /// <param name="nativeResult">
    /// The result of the query.  The caller loses ownership of this object: it is either
    /// transferred to the new instance of <see cref="DuckDbResult" />, or otherwise (when this
    /// method throws an exception) gets destroyed.
    /// </param>
    /// <returns>
    /// If <paramref name="status" /> indicates success, an instance of <see cref="DuckDbResult" />
    /// that wraps the native result object.
    /// </returns>
    internal static DuckDbResult CreateFromQuery(duckdb_state status, ref duckdb_result nativeResult)
    {
        try
        {
            if (status != duckdb_state.DuckDBSuccess)
                throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));

            // Passes ownership of nativeResult
            return new DuckDbResult(ref nativeResult);
        }
        catch
        {
            NativeMethods.duckdb_destroy_result(ref nativeResult);
            throw;
        }
    }

    #endregion

    #region Summary of results

    /// <summary>
    /// Get the number of changed rows (if this result object is not fronm a pure query).
    /// </summary>
    /// <returns>
    /// Used to implement <see cref="DuckDbDataReader.RecordsAffected" />
    /// and similar properties/methods.
    /// </returns>
    internal long GetNumberOfChangedRows(out bool hasResultRows)
    {
        using var _ = _refCount.EnterScope(this);
        return ExtractNumberOfChangedRows(ref _nativeResult, out hasResultRows, destroyNativeResult: false);
    }

    #endregion

    #region Summary processing of results without creating result object

    private static long ExtractNumberOfChangedRows(ref duckdb_result nativeResult, 
                                                   out bool hasResultRows,
                                                   bool destroyNativeResult)
    {
        try
        {
            var resultType = NativeMethods.duckdb_result_return_type(nativeResult);

            hasResultRows = false;
            switch (resultType)
            {
                case duckdb_result_type.DUCKDB_RESULT_TYPE_INVALID:
                    throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));

                case duckdb_result_type.DUCKDB_RESULT_TYPE_CHANGED_ROWS:
                    return NativeMethods.duckdb_rows_changed(ref nativeResult);

                case duckdb_result_type.DUCKDB_RESULT_TYPE_QUERY_RESULT:
                    hasResultRows = true;
                    break;
            }

            return -1;
        }
        finally
        {
            if (destroyNativeResult)
                NativeMethods.duckdb_destroy_result(ref nativeResult);
        }
    }

    /// <summary>
    /// Extract the number of changed rows from executing some SQL statement, and
    /// abandon the native result object.
    /// </summary>
    /// <remarks>
    /// This method is common code used to implement <see cref="DuckDbConnection.ExecuteNonQuery" />
    /// and <see cref="DuckDbCommand.ExecuteNonQuery" />.
    /// </remarks>
    internal static long TakeNumberOfChangedRows(ref duckdb_result nativeResult)
        => ExtractNumberOfChangedRows(ref nativeResult, out _, destroyNativeResult: true);

    /// <summary>
    /// Extract the value at the first row and column, if it exists.
    /// </summary>
    /// <remarks>
    /// Used to implement <see cref="DuckDbConnection.ExecuteScalar(string)" />
    /// and similar methods.
    /// </remarks>
    internal static T? ExtractFirstCell<T>(duckdb_state status, ref duckdb_result nativeResult)
    {
        try
        {
            if (status != duckdb_state.DuckDBSuccess)
                throw new DuckDbException(NativeMethods.duckdb_result_error(ref nativeResult));
            var resultType = NativeMethods.duckdb_result_return_type(nativeResult);
            if (resultType != duckdb_result_type.DUCKDB_RESULT_TYPE_QUERY_RESULT)
                return default;

            var nativeChunk = NativeMethods.duckdb_fetch_chunk(nativeResult);
            if (nativeChunk == null)
                return default;

            try
            {
                var length = (int)NativeMethods.duckdb_data_chunk_get_size(nativeChunk);
                if (length <= 0)
                    return default;

                var nativeVector = NativeMethods.duckdb_data_chunk_get_vector(nativeChunk, 0);
                if (nativeVector == null)
                    return default;

                var columnInfo = new DuckDbColumnInfo(ref nativeResult, 0);
                var vectorInfo = new DuckDbVectorInfo(nativeVector, length, columnInfo);

                var reader = new DuckDbVectorReader<T>(vectorInfo);
                bool isValid = reader.TryGetItem(0, out var item);
                if (!isValid && !reader.DefaultValueIsInvalid)
                {
                    throw new InvalidOperationException(
                        "The DuckDB query returned null, which cannot be represented " +
                        "as an instance of the type T that the generic method " +
                        "ExecuteValue<T> has been invoked with.  Consider replacing " +
                        "the generic parameter with T? (System.Nullable<T>) instead. ");
                }

                return item;
            }
            finally
            {
                NativeMethods.duckdb_destroy_data_chunk(ref nativeChunk);
            }
        }
        finally
        {
            NativeMethods.duckdb_destroy_result(ref nativeResult);
        }
    }

    #endregion

    #region Getting and processing chunks

    /// <summary>
    /// Retrieve the next chunk of results from the present query in DuckDB.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method may not be called simultaneously from multiple threads.
    /// </para>
    /// <para>
    /// Nevertheless, the processing of the contents of chunks may still be parallelized.
    /// Have one thread/task call this method repeatedly to obtain individual
    /// chunk objects.  Then, each such object may be passed to a different thread/task,
    /// to process its contents via <see cref="DuckDbResultChunk.ProcessContents" />.
    /// </para>
    /// </remarks>
    /// <returns>
    /// Object containing the next chunk, or null if there are no more chunks.
    /// </returns>
    public DuckDbResultChunk? FetchNextChunk()
    {
        _duckdb_data_chunk* nativeChunk;
        using (var rs = _refCount.EnterScope(this))
        {
            rs.ThrowIfNotFirst();
            nativeChunk = NativeMethods.duckdb_fetch_chunk(_nativeResult);
        }

        if (nativeChunk == null)
            return null;    // exhausted all results

        try
        {
            return new DuckDbResultChunk(ref nativeChunk, this);
        }
        catch
        {
            NativeMethods.duckdb_destroy_data_chunk(ref nativeChunk);
            throw;
        }
    }

    /// <summary>
    /// Process the next chunk of results from the present query in DuckDB, 
    /// with a caller-specified function. 
    /// </summary>
    /// <typeparam name="TState">
    /// Type of arbitrary state to pass into the caller-specified function.
    /// </typeparam>
    /// <typeparam name="TReturn">
    /// The type of value returned by the caller-specified function.
    /// </typeparam>
    /// <param name="state">
    /// The state object or structure to pass into <paramref name="function" />.
    /// </param>
    /// <param name="function">
    /// The caller-specified function that receives the results from the next chunk
    /// and may do any processing on it.
    /// </param>
    /// <param name="result">
    /// On return, set to the whatever <paramref name="function" /> returns.
    /// If there is no next chunk, this argument is set to the default value
    /// for its type.
    /// </param>
    /// <returns>
    /// True when a chunk has been successfully processed.  False when
    /// there are no more chunks.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method offers fast, direct access to the native memory backing the 
    /// DuckDB vectors (columns) of the results.  
    /// However, to make these operations safe (allowing no dangling pointers), 
    /// this library must be able to bound the scope of access.  Thus, the code
    /// to consume the vectors' data must be encapsulated in a function that this
    /// method invokes. 
    /// </para>
    /// <para>
    /// A chunk processed by this method is discarded immediately afterwards
    /// (even if <paramref name="function" /> fails with an exception).
    /// To work with the same chunk again, the query must be re-executed anew.
    /// </para>
    /// <para>
    /// Alternatively, use the method <see cref="FetchNextChunk" /> instead
    /// which returns the next chunk as a standalone object, 
    /// that can be processed over and over again.
    /// </para>
    /// <para>
    /// This method can be called continually, until it returns false, 
    /// to process all chunks of the result.
    /// </para>
    /// <para>
    /// This method may not be called from multiple threads simultaneously.
    /// Although there is potential for multiple threads to process the contents of
    /// distinct chunks in parallel, getting (fetching) successive chunks
    /// is not parallelizable, and the API shape of this method does not allow
    /// segregating the two concerns.  
    /// Use the chunk objects <see cref="DuckDbResultChunk" />
    /// from <see cref="FetchNextChunk" /> instead if multi-thread processing is desired.
    /// </para>
    /// </remarks>
    public bool ProcessNextChunk<TState, TReturn>(TState state, 
                                                  DuckDbChunkReadingFunc<TState, TReturn> function,
                                                  [MaybeNullWhen(false)] out TReturn result)
        where TState : allows ref struct
    {
        _duckdb_data_chunk* nativeChunk;
        using (var rs = _refCount.EnterScope(this))
        {
            rs.ThrowIfNotFirst();
            nativeChunk = NativeMethods.duckdb_fetch_chunk(_nativeResult);
        }

        if (nativeChunk == null)
        {
            result = default;
            return false;
        }

        try
        {
            var length = (int)NativeMethods.duckdb_data_chunk_get_size(nativeChunk);
            var reader = new DuckDbChunkReader(nativeChunk, this, length);
            result = function(reader, state);
            return true;
        }
        finally
        {
            NativeMethods.duckdb_destroy_data_chunk(ref nativeChunk);
        }
    }

    /// <summary>
    /// Process all the following chunks of results from the present query in DuckDB, 
    /// with a caller-specified function. 
    /// </summary>
    /// <typeparam name="TState">
    /// Type of arbitrary state to pass into the caller-specified function.
    /// </typeparam>
    /// <typeparam name="TReturn">
    /// The type of value returned by the caller-specified function.
    /// </typeparam>
    /// <param name="state">
    /// The state object or structure to pass into <paramref name="function" />.
    /// </param>
    /// <param name="function">
    /// The caller-specified function that receives the results from the next chunk
    /// and may do any processing on it.
    /// </param>
    /// <returns>
    /// The return value is whatever <paramref name="function" /> returns
    /// for the last chunk (if successful).  
    /// If there are no more chunks, this argument is set to the default value
    /// for its type.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method offers fast, direct access to the native memory backing the 
    /// DuckDB vectors (columns) of the results.  
    /// However, to make these operations safe (allowing no dangling pointers), 
    /// this library must be able to bound the scope of access.  Thus, the code
    /// to consume the vectors' data must be encapsulated in a function that this
    /// method invokes. 
    /// </para>
    /// <para>
    /// The chunks processed by this method are discarded afterwards.
    /// To work with the results again, the query must be re-executed anew.
    /// </para>
    /// <para>
    /// This method is equivalent to calling <see cref="ProcessNextChunk" />
    /// in a loop until that method returns false.
    /// </para>
    /// <para>
    /// The return values of <paramref name="function" /> for every chunk
    /// except the last are discarded.  To communicate information between
    /// successive invocations of <paramref name="function" />, pass in either
    /// a reference type, or a "ref struct" containing managed pointers,
    /// for <typeparamref name="TState" /> so that <paramref name="function" />
    /// can modify the referents.  Or, of course, the code for <paramref name="function" />
    /// could also be written to close over individual variables from its surrounding
    /// scope.
    /// </para>
    /// <para>
    /// This method may not be called from multiple threads simultaneously.
    /// </para>
    /// </remarks>
    public TReturn? ProcessAllChunks<TState, TReturn>(TState state, 
                                                      DuckDbChunkReadingFunc<TState, TReturn> function)
        where TState : allows ref struct
    {
        TReturn? result;
        bool hasChunk;
        do
        {
            hasChunk = ProcessNextChunk(state, function, out result);
        } while (hasChunk);

        return result;
    }

    /// <summary>
    /// Process all the following chunks of results from the present query in DuckDB, 
    /// by invocating a caller-specified function on each, 
    /// and accumulate return values across invocations.
    /// </summary>
    /// <typeparam name="TState">
    /// Type of arbitrary state to pass into the caller-specified function.
    /// </typeparam>
    /// <typeparam name="TReturn">
    /// The type of value returned by the caller-specified function.
    /// </typeparam>
    /// <param name="state">
    /// The state object or structure to pass into <paramref name="function" />.
    /// </param>
    /// <param name="function">
    /// The caller-specified function that receives the results from the next chunk
    /// and may do any processing on it.
    /// </param>
    /// <param name="accumulate">
    /// The function to accumulate (or aggregate, or "reduce") the return values
    /// from successive invocations of <paramref name="function" />.  The first
    /// argument is the previous accumulated value; the second argument is the
    /// return value from the next chunk.  For the first invocation, the first
    /// argument is <paramref name="seed" />.
    /// </param>
    /// <param name="seed">
    /// The initial value for the accumulation of return values from <paramref name="function" />.
    /// </param>
    /// <returns>
    /// The accumulated value of the return values from the chunks.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This method offers fast, direct access to the native memory backing the 
    /// DuckDB vectors (columns) of the results.  
    /// However, to make these operations safe (allowing no dangling pointers), 
    /// this library must be able to bound the scope of access.  Thus, the code
    /// to consume the vectors' data must be encapsulated in a function that this
    /// method invokes. 
    /// </para>
    /// <para>
    /// The chunks processed by this method are discarded afterwards.
    /// To work with the results again, the query must be re-executed anew.
    /// </para>
    /// <para>
    /// This method is equivalent to calling <see cref="ProcessNextChunk" />
    /// in a loop until that method returns false.
    /// </para>
    /// <para>
    /// This method may not be called from multiple threads simultaneously.
    /// </para>
    /// </remarks>
    public TReturn ProcessAllChunks<TState, TReturn>(TState state,
                                                     DuckDbChunkReadingFunc<TState, TReturn> function,
                                                     Func<TReturn, TReturn, TReturn> accumulate,
                                                     TReturn seed)

        where TState : allows ref struct
    {
        TReturn result = seed;
        while (true)
        {
            bool hasChunk = ProcessNextChunk(state, function, out var value);
            if (!hasChunk)
                break;

            // About the null-silencing operator here:  Not sure why the C# compiler is not
            // seeing that when this line is executed, hasChunk is true and therefore TReturn
            // from ProcessNextChunk should not be null (for the purposes of null ref. analysis).
            result = accumulate(result, value!);
        }

        return result;
    }

    #endregion

    #region Result columns

    /// <summary>
    /// Top-level information gathered/cached on each column. 
    /// </summary>
    private struct Column
    {
        /// <summary>
        /// Basic type information.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This data, once initialized, is immutable and does not 
        /// involve any native resources from DuckDB.  Therefore, this data is not subject to the 
        /// multi-thread access restrictions.
        /// </para>
        /// </remarks>
        public DuckDbColumnInfo Info { get; init; }

        /// <summary>
        /// The name of the column from DuckDB.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This name is retrieved only if requested by the user,
        /// as that is a mildly heavy operation (requiring a temporary memory allocation from the
        /// native C API, and then conversion from UTF-8 into a .NET string), and it is not needed
        /// for decoding data.
        /// </para>
        /// </remarks>
        public string? Name;

        /// <summary>
        /// Backing field for <see cref="Converter" /> implementing atomic read/write.
        /// </summary>
        private Antitear<VectorElementConverter> _converter;

        /// <summary>
        /// Converter for the items in the column, created and cached on first access.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The interface of <see cref="DuckDbChunkReader" /> in theory allows different types
        /// to be selected each time a chunk is processed, but in practice all the vectors
        /// (across all chunks) from one column always use the same converter.  There is no point
        /// in making the user "pre-register" the converter for each column before any vectors
        /// are accessed.
        /// </para>
        /// <para>
        /// We just check <see cref="VectorElementConverter.TargetType" /> for any existing cached 
        /// instance to know if the instance is still applicable. 
        /// </para>
        /// </remarks>
        public VectorElementConverter Converter 
        { 
            get => _converter.Value; 
            set => _converter.Value = value; 
        }
    }

    /// <summary>
    /// Top-level information gathered/cached on the columns of the result.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The elements of this array (deliberately) do not hold any pointers to DuckDB native objects or memory,
    /// so read-only access does not require entering the reference-count scope.  
    /// </para>
    /// </remarks>
    private readonly Column[] _columns;

    /// <summary>
    /// The number of columns present in the result.
    /// </summary>
    public int ColumnCount => _columns.Length;

    /// <summary>
    /// Get information about a column in the results.
    /// </summary>
    /// <param name="columnIndex">
    /// The index of the column, between 0 (inclusive) to <see cref="ColumnCount" /> (exclusive).
    /// </param>
    public DuckDbColumnInfo GetColumnInfo(int columnIndex) => _columns[columnIndex].Info;

    /// <summary>
    /// Get the name of a column in the results.
    /// </summary>
    /// <param name="columnIndex">
    /// The index of the column, between 0 (inclusive) to <see cref="ColumnCount" /> (exclusive).
    /// </param>
    /// <returns>
    /// The name of the column, or <see cref="string.Empty" /> if it has no name.
    /// </returns>
    public string GetColumnName(int columnIndex)
    {
        ref string? nameRef = ref _columns[columnIndex].Name;
        var name = nameRef;

        if (name == null)
        {
            using var _ = _refCount.EnterScope(this);
            name = NativeMethods.duckdb_column_name(ref _nativeResult, columnIndex);

            // Always return the first string constructed if there is a race.
            name = Interlocked.CompareExchange(ref nameRef, name, null) ?? name;
        }

        return name;
    }

    VectorElementConverter IResultColumns.GetColumnConverter(int columnIndex, Type targetType)
    {
        ref var column = ref _columns[columnIndex];
        VectorElementConverter converter;

        // Read from cache
        converter = column.Converter;

        // Cache miss
        if (!(converter.IsValid && converter.TargetType == targetType))
        {
            using (var _ = _refCount.EnterScope(_nativeResult))
            {
                var descriptor = new ConverterCreationContext.ColumnDescriptor(ref _nativeResult, columnIndex);
                var context = ConverterCreationContext.FromColumn(column.Info, ref descriptor);

                converter = VectorElementConverter.CreateForType(targetType, in context);
            }

            if (!converter.IsValid)
                DuckDbVectorInfo.ThrowForWrongParamType(column.Info, targetType ?? typeof(object));

            column.Converter = converter;
        }

        return converter;
    }

    #endregion
}
