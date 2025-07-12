using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;

using idx_t = long;

namespace DuckDB.C_API;

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

public enum duckdb_type : int
{
    DUCKDB_TYPE_INVALID = 0,
    // bool
    DUCKDB_TYPE_BOOLEAN = 1,
    // int8_t
    DUCKDB_TYPE_TINYINT = 2,
    // int16_t
    DUCKDB_TYPE_SMALLINT = 3,
    // int32_t
    DUCKDB_TYPE_INTEGER = 4,
    // int64_t
    DUCKDB_TYPE_BIGINT = 5,
    // uint8_t
    DUCKDB_TYPE_UTINYINT = 6,
    // uint16_t
    DUCKDB_TYPE_USMALLINT = 7,
    // uint32_t
    DUCKDB_TYPE_UINTEGER = 8,
    // uint64_t
    DUCKDB_TYPE_UBIGINT = 9,
    // float
    DUCKDB_TYPE_FLOAT = 10,
    // double
    DUCKDB_TYPE_DOUBLE = 11,
    // duckdb_timestamp (microseconds)
    DUCKDB_TYPE_TIMESTAMP = 12,
    // duckdb_date
    DUCKDB_TYPE_DATE = 13,
    // duckdb_time
    DUCKDB_TYPE_TIME = 14,
    // duckdb_interval
    DUCKDB_TYPE_INTERVAL = 15,
    // duckdb_hugeint
    DUCKDB_TYPE_HUGEINT = 16,
    // duckdb_uhugeint
    DUCKDB_TYPE_UHUGEINT = 32,
    // const char*
    DUCKDB_TYPE_VARCHAR = 17,
    // duckdb_blob
    DUCKDB_TYPE_BLOB = 18,
    // duckdb_decimal
    DUCKDB_TYPE_DECIMAL = 19,
    // duckdb_timestamp_s (seconds)
    DUCKDB_TYPE_TIMESTAMP_S = 20,
    // duckdb_timestamp_ms (milliseconds)
    DUCKDB_TYPE_TIMESTAMP_MS = 21,
    // duckdb_timestamp_ns (nanoseconds)
    DUCKDB_TYPE_TIMESTAMP_NS = 22,
    // enum type, only useful as logical type
    DUCKDB_TYPE_ENUM = 23,
    // list type, only useful as logical type
    DUCKDB_TYPE_LIST = 24,
    // struct type, only useful as logical type
    DUCKDB_TYPE_STRUCT = 25,
    // map type, only useful as logical type
    DUCKDB_TYPE_MAP = 26,
    // duckdb_array, only useful as logical type
    DUCKDB_TYPE_ARRAY = 33,
    // duckdb_hugeint
    DUCKDB_TYPE_UUID = 27,
    // union type, only useful as logical type
    DUCKDB_TYPE_UNION = 28,
    // duckdb_bit
    DUCKDB_TYPE_BIT = 29,
    // duckdb_time_tz
    DUCKDB_TYPE_TIME_TZ = 30,
    // duckdb_timestamp (microseconds)
    DUCKDB_TYPE_TIMESTAMP_TZ = 31,
    // enum type, only useful as logical type
    DUCKDB_TYPE_ANY = 34,
    // duckdb_varint
    DUCKDB_TYPE_VARINT = 35,
    // enum type, only useful as logical type
    DUCKDB_TYPE_SQLNULL = 36,
    // enum type, only useful as logical type
    DUCKDB_TYPE_STRING_LITERAL = 37,
    // enum type, only useful as logical type
    DUCKDB_TYPE_INTEGER_LITERAL = 38,
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

internal unsafe struct _duckdb_connection { private void* internal_ptr; }
internal unsafe struct _duckdb_database { private void* internal_ptr; }
internal unsafe struct _duckdb_config { private void* internal_ptr; }

internal unsafe struct _duckdb_value { private void* internal_ptr; }
internal unsafe struct _duckdb_data_chunk { private void* internal_ptr; }
internal unsafe struct _duckdb_vector { private void* internal_ptr; }

public struct DuckDbTimestamp
{
    public long Microseconds;
}

public struct DuckDbDate
{
    public int Days; 
}


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
                                                         [MarshalUsing(typeof(FreeStringMarshaller))] out string out_error);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void duckdb_close(ref _duckdb_database* database);

    #endregion

    #region Database configuration

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_create_config(out _duckdb_config* config);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_set_config(_duckdb_config* config, string name, string option);


    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void duckdb_destroy_config(ref _duckdb_config* config);

    #endregion

    #region Miscellaneous
    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void duckdb_free(void* ptr);

    #endregion

    #region Establishing database connections

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_connect(_duckdb_database* database, out _duckdb_connection* connection);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void duckdb_disconnect(ref _duckdb_connection* connection);

    #endregion

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void duckdb_interrupt(_duckdb_connection* connection);
//    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
//    internal static partial duckdb_query_progress_type duckdb_query_progress(_duckdb_connection* connection);
    //    internal static partial void duckdb_connection_get_client_context(_duckdb_connection* connection, duckdb_client_context* out_context);
    //    internal static partial idx_t duckdb_client_context_get_connection_id(duckdb_client_context context);
    //    internal static partial void duckdb_destroy_client_context(duckdb_client_context* context);
    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial /* const */ char* duckdb_library_version();
    //    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    //    internal static partial _duckdb_value* duckdb_get_table_names(_duckdb_connection* connection, /* const */ char* query, bool qualified);

    #region Executing queries

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_query(_duckdb_connection* connection, string query, out duckdb_result result);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void duckdb_destroy_result(ref duckdb_result result);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial string duckdb_result_error(ref duckdb_result result);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial idx_t duckdb_column_count(ref duckdb_result result);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial string duckdb_column_name(ref duckdb_result result, idx_t col);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_type duckdb_column_type(ref duckdb_result result, idx_t col);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial idx_t duckdb_rows_changed(ref duckdb_result result);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_result_type duckdb_result_return_type(duckdb_result result);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial _duckdb_data_chunk* duckdb_fetch_chunk(duckdb_result result);

    #endregion

    #region Data chunks

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void duckdb_destroy_data_chunk(ref _duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial idx_t duckdb_data_chunk_get_column_count(_duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial idx_t duckdb_data_chunk_get_size(_duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial _duckdb_vector* duckdb_data_chunk_get_vector(_duckdb_data_chunk* chunk, idx_t col_idx);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void* duckdb_vector_get_data(_duckdb_vector* vector);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial ulong* duckdb_vector_get_validity(_duckdb_vector* vector);

    #endregion
}

#pragma warning restore IDE1006 // Naming Styles
