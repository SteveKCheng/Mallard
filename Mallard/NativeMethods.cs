using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;

using idx_t = long;

[assembly: DisableRuntimeMarshalling]

namespace Mallard.C_API;

#pragma warning disable IDE1006, CS0169 // Naming Styles, unused struct members

internal enum duckdb_state : int
{
    DuckDBSuccess = 0,
    DuckDBError = 1
}

internal enum duckdb_result_type : int
{
    DUCKDB_RESULT_TYPE_INVALID = 0,
    DUCKDB_RESULT_TYPE_CHANGED_ROWS = 1,
    DUCKDB_RESULT_TYPE_NOTHING = 2,
    DUCKDB_RESULT_TYPE_QUERY_RESULT = 3,
}

//! An enum over DuckDB's different error types.
internal enum duckdb_error_type
{
    DUCKDB_ERROR_INVALID = 0,
    DUCKDB_ERROR_OUT_OF_RANGE = 1,
    DUCKDB_ERROR_CONVERSION = 2,
    DUCKDB_ERROR_UNKNOWN_TYPE = 3,
    DUCKDB_ERROR_DECIMAL = 4,
    DUCKDB_ERROR_MISMATCH_TYPE = 5,
    DUCKDB_ERROR_DIVIDE_BY_ZERO = 6,
    DUCKDB_ERROR_OBJECT_SIZE = 7,
    DUCKDB_ERROR_INVALID_TYPE = 8,
    DUCKDB_ERROR_SERIALIZATION = 9,
    DUCKDB_ERROR_TRANSACTION = 10,
    DUCKDB_ERROR_NOT_IMPLEMENTED = 11,
    DUCKDB_ERROR_EXPRESSION = 12,
    DUCKDB_ERROR_CATALOG = 13,
    DUCKDB_ERROR_PARSER = 14,
    DUCKDB_ERROR_PLANNER = 15,
    DUCKDB_ERROR_SCHEDULER = 16,
    DUCKDB_ERROR_EXECUTOR = 17,
    DUCKDB_ERROR_CONSTRAINT = 18,
    DUCKDB_ERROR_INDEX = 19,
    DUCKDB_ERROR_STAT = 20,
    DUCKDB_ERROR_CONNECTION = 21,
    DUCKDB_ERROR_SYNTAX = 22,
    DUCKDB_ERROR_SETTINGS = 23,
    DUCKDB_ERROR_BINDER = 24,
    DUCKDB_ERROR_NETWORK = 25,
    DUCKDB_ERROR_OPTIMIZER = 26,
    DUCKDB_ERROR_NULL_POINTER = 27,
    DUCKDB_ERROR_IO = 28,
    DUCKDB_ERROR_INTERRUPT = 29,
    DUCKDB_ERROR_FATAL = 30,
    DUCKDB_ERROR_INTERNAL = 31,
    DUCKDB_ERROR_INVALID_INPUT = 32,
    DUCKDB_ERROR_OUT_OF_MEMORY = 33,
    DUCKDB_ERROR_PERMISSION = 34,
    DUCKDB_ERROR_PARAMETER_NOT_RESOLVED = 35,
    DUCKDB_ERROR_PARAMETER_NOT_ALLOWED = 36,
    DUCKDB_ERROR_DEPENDENCY = 37,
    DUCKDB_ERROR_HTTP = 38,
    DUCKDB_ERROR_MISSING_EXTENSION = 39,
    DUCKDB_ERROR_AUTOLOAD = 40,
    DUCKDB_ERROR_SEQUENCE = 41,
    DUCKDB_INVALID_CONFIGURATION = 42
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_connection { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_database { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_config { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_prepared_statement { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_value { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_data_chunk { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_vector { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_logical_type { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct duckdb_varint 
{
    internal byte* data;
    internal idx_t size;
    internal bool is_negative;
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct duckdb_result
{
    private idx_t deprecated_column_count;
    private idx_t deprecated_row_count;
    private idx_t deprecated_rows_changed;
    private void* deprecated_columns;
    private void* deprecated_error_message;
    private void* internal_data;
}

internal unsafe static partial class NativeMethods
{
    private const string LibraryName = "duckdb";

    #region Opening databases

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_open(string path, out _duckdb_database* database);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_open_ext(string path, 
                                                         out _duckdb_database* database, 
                                                         _duckdb_config* config, 
                                                         [MarshalUsing(typeof(Utf8StringMarshallerWithFree))] out string out_error);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_close(ref _duckdb_database* database);

    #endregion

    #region Database configuration

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_create_config(out _duckdb_config* config);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_set_config(_duckdb_config* config, string name, string option);


    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_config(ref _duckdb_config* config);

    #endregion

    #region Miscellaneous

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_free(void* ptr);

    [LibraryImport(LibraryName)]
    internal static partial long duckdb_vector_size();

    #endregion

    #region Establishing database connections

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_connect(_duckdb_database* database, out _duckdb_connection* connection);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_disconnect(ref _duckdb_connection* connection);

    #endregion

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_interrupt(_duckdb_connection* connection);

    //    internal static partial duckdb_query_progress_type duckdb_query_progress(_duckdb_connection* connection);
    //    internal static partial void duckdb_connection_get_client_context(_duckdb_connection* connection, duckdb_client_context* out_context);
    //    internal static partial idx_t duckdb_client_context_get_connection_id(duckdb_client_context context);
    //    internal static partial void duckdb_destroy_client_context(duckdb_client_context* context);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithoutFree))]
    internal static partial string duckdb_library_version();

    #region Executing queries

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_query(_duckdb_connection* connection, string query, out duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_execute_prepared(_duckdb_prepared_statement* prepared_statement,
                                                                 out duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_result(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithoutFree))]
    internal static partial string duckdb_result_error(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_column_count(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithoutFree))]
    internal static partial string duckdb_column_name(ref duckdb_result result, idx_t col);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbBasicType duckdb_column_type(ref duckdb_result result, idx_t col);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_rows_changed(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_result_type duckdb_result_return_type(duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_data_chunk* duckdb_fetch_chunk(duckdb_result result);

    #endregion

    #region Data chunks

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_data_chunk(ref _duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_data_chunk_get_column_count(_duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_data_chunk_get_size(_duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_vector* duckdb_data_chunk_get_vector(_duckdb_data_chunk* chunk, idx_t col_idx);

    #endregion

    #region Vectors

    [LibraryImport(LibraryName)]
    internal static partial void* duckdb_vector_get_data(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial ulong* duckdb_vector_get_validity(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_vector* duckdb_array_vector_get_child(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_vector* duckdb_list_vector_get_child(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_list_vector_get_size(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_vector* duckdb_struct_vector_get_child(_duckdb_vector* vector, idx_t index);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_vector_get_column_type(_duckdb_vector* vector);

    #endregion

    #region Prepared statements
    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_prepare(_duckdb_connection* connection, string query, out _duckdb_prepared_statement* prepared_statement);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_prepare(ref _duckdb_prepared_statement* prepared_statement);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithoutFree))]
    internal static partial string duckdb_prepare_error(_duckdb_prepared_statement* prepared_statement);

    [LibraryImport(LibraryName)]
    internal static partial long duckdb_nparams(_duckdb_prepared_statement* parepared_statement);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithFree))]
    internal static partial string duckdb_parameter_name(_duckdb_prepared_statement* parepared_statement, idx_t index);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbBasicType duckdb_param_type(_duckdb_prepared_statement* parepared_statement, idx_t param_idx);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_bind_parameter_index(_duckdb_prepared_statement* prepared_statement,
                                                                     out idx_t param_idx,
                                                                     string name);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_value(_duckdb_prepared_statement* prepared_statement,
                                                           idx_t param_idx,
                                                           _duckdb_value* val);

    #endregion

    #region Logical types

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_logical_type(ref _duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbBasicType duckdb_get_type_id(_duckdb_logical_type* type);

    #endregion

    #region Objects for single values

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_value(ref _duckdb_value* value);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_varchar_length(byte* text, idx_t length);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_bool([MarshalAs(UnmanagedType.I1)] bool input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_int8(sbyte input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_uint8(byte input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_int16(short input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_uint16(ushort input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_int32(int input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_uint32(uint input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_uint64(ulong input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_int64(long val);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_date(DuckDbDate input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_timestamp(DuckDbTimestamp input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_hugeint(Int128 input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_uhugeint(UInt128 input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_varint(
        [MarshalUsing(typeof(BigIntegerMarshaller))] BigInteger input);

    /*
    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_decimal(duckdb_decimal input);
    */

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_float(float input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_double(double input);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithFree))]
    internal static partial string duckdb_value_to_string(_duckdb_value* value);

    #endregion 
}

#pragma warning restore IDE1006, CS0169
