using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;

using idx_t = long;

[assembly: DisableRuntimeMarshalling]

namespace Mallard.Interop;
using Mallard.Types;

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

// Notes on how "object" types from the DuckDB C API are imported internally in Mallard:
//
// In DuckDB's header file (<c>duckdb.h</c>), <c>typedef</c> is for opaque pointers to various kinds
// of "objects", as is standard in well-designed C APIs which hide the ABI details of those objects
// from the client.
//
// While theoretically, C#'s type aliases (<c>using type_alias = …</c>) work like <c>typedef</c> in C,
// they are not part of the .NET type system, and their definitions have to be duplicated on all source
// files that use them.  (C# has standard "header inclusion" facility.)
//
// So we do not use C# type alias, instead writing out what the <c>typedef</c> expands to at every
// use.  Typically the <c>typedef</c> in C refers to a pointer to a <c>struct</c> tag.  (Recall that in C,
// unlike C++, <c>struct</c> tags live in a different than actual types.)  We repeat the definitions
// of those <c>struct</c> types in C#, named with an underscore prefix (<c>_</c>).  We do not manipulate
// the data contents of these structures directly of course.  For all we know, the definitions might be "fake"
// in the sense that DuckDB internally uses a different definition of the structure, whose instances are
// heap-allocated, and we just have to store or load *pointers* to them.  To emphasize, our C# code would 
// work with and store values of type <c>_duckdb_connection*</c> and never of type <c>_duckdb_connection</c>.
//
// Pay attention to the number of pointer indirections in the DuckDB C API functions.  The number of
// "stars" (<c>*</c> de-references) may not match what is superficially apparent.
//
// Especially, watch out for some older functions in the DuckDB C API where the client is expected to *store*
// the entire data structure, i.e. the data members of the structure are stored in the C# stack or even
// as part of a heap-allocated C# object.  Some of those are design mistakes in the DuckDB C API but
// have to stick around for compatibility reasons.  Others are intentional because the client is expected
// to work with the data members directly, for efficiency reasons, such as <c>duckdb_bignum</c>.
// Note that the corresponding <c>typedef</c>s for those types generally refer to the <c>struct</c>
// itself in <c>duckdb.h</c>.  In our C# codebase, such structures are named *without* the underscore
// prefix, i.e. the name of the type in C# is the same as the <c>typedef</c> name in C.  
//

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
internal unsafe struct _duckdb_appender { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct _duckdb_error_data { private void* internal_ptr; }

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct duckdb_bignum 
{
    internal byte* data;
    internal idx_t size;
    internal bool is_negative;
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct duckdb_bit
{
    internal byte* data;
    internal idx_t size;
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

    // Same as above but allows passing constant queries embedded in this library,
    // in UTF-8 encoding, without re-encoding it.
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_query(_duckdb_connection* connection, byte* query,
                                                      out duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_execute_prepared(_duckdb_prepared_statement* prepared_statement,
                                                                 out duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_result(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithoutFree))]
    internal static partial string duckdb_result_error(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbErrorKind duckdb_result_error_type(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_column_count(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithoutFree))]
    internal static partial string duckdb_column_name(ref duckdb_result result, idx_t col);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbValueKind duckdb_column_type(ref duckdb_result result, idx_t col);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_column_logical_type(ref duckdb_result result, idx_t col);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_rows_changed(ref duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_result_type duckdb_result_return_type(duckdb_result result);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_data_chunk* duckdb_fetch_chunk(duckdb_result result);
    
    #endregion

    #region Data chunks

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_data_chunk* duckdb_create_data_chunk(_duckdb_logical_type** types, idx_t column_count);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_data_chunk(ref _duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_data_chunk_reset(_duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_data_chunk_get_column_count(_duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_data_chunk_get_size(_duckdb_data_chunk* chunk);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_data_chunk_set_size(_duckdb_data_chunk* chunk, idx_t size);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_vector* duckdb_data_chunk_get_vector(_duckdb_data_chunk* chunk, idx_t col_idx);

    #endregion

    #region Vectors

    [LibraryImport(LibraryName)]
    internal static partial void* duckdb_vector_get_data(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial ulong* duckdb_vector_get_validity(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_vector_ensure_validity_writable(_duckdb_vector* vector);

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_validity_set_row_invalid(ulong* validity, idx_t row);

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

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_vector_assign_string_element_len(_duckdb_vector* vector, idx_t index, byte* str, idx_t str_len);

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
    internal static partial DuckDbValueKind duckdb_param_type(_duckdb_prepared_statement* parepared_statement, idx_t param_idx);
    
    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_param_logical_type(_duckdb_prepared_statement* prepared_statement, idx_t param_idx);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_bind_parameter_index(_duckdb_prepared_statement* prepared_statement,
                                                                     out idx_t param_idx,
                                                                     string name);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_value(_duckdb_prepared_statement* prepared_statement,
                                                           idx_t param_idx,
                                                           _duckdb_value* val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_clear_bindings(_duckdb_prepared_statement* prepared_statement);

    #endregion

    #region Binding values to parameters of prepared statements

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_boolean(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, [MarshalAs(UnmanagedType.I1)] bool val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_int8(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, sbyte val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_int16(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, short val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_int32(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, int val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_int64(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, long val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_hugeint(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbUInt128 val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_uhugeint(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbUInt128 val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_decimal(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbDecimal val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_uint8(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, byte val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_uint16(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, ushort val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_uint32(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, uint val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_uint64(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, ulong val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_float(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, float val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_double(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, double val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_date(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbDate val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_time(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbTime val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_timestamp(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbTimestamp val);
    
    // [LibraryImport(LibraryName)]
    // internal static partial duckdb_state duckdb_bind_timestamp_tz(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbTimestamp val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_interval(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, DuckDbInterval val);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_varchar_length(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, byte* val, idx_t length);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_blob(_duckdb_prepared_statement* prepared_statement, idx_t param_idx, void* data, idx_t length);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_bind_null(_duckdb_prepared_statement* prepared_statement, idx_t param_idx);    
    
    #endregion
    
    #region Logical types

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_create_logical_type(DuckDbValueKind type);
    
    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_logical_type(ref _duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithFree))]
    internal static partial string duckdb_logical_type_get_alias(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbValueKind duckdb_get_type_id(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial byte duckdb_decimal_width(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial byte duckdb_decimal_scale(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbValueKind duckdb_decimal_internal_type(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_array_type_array_size(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_list_type_child_type(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_array_type_child_type(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_struct_type_child_count(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithFree))]
    internal static partial string duckdb_struct_type_child_name(_duckdb_logical_type* type, idx_t index);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_struct_type_child_type(_duckdb_logical_type* type, idx_t index);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_union_type_member_count(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithFree))]
    internal static partial string duckdb_union_type_member_name(_duckdb_logical_type* type, idx_t index);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbValueKind duckdb_enum_internal_type(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    internal static partial uint duckdb_enum_dictionary_size(_duckdb_logical_type* type);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithFree))]
    internal static partial string duckdb_enum_dictionary_value(_duckdb_logical_type* type, idx_t index);

    #endregion

    #region Objects for single values

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_value(ref _duckdb_value* value);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_varchar_length(byte* text, idx_t length);
    
    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_blob(byte *data, idx_t length);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_bit(duckdb_bit input);

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
    internal static partial _duckdb_value* duckdb_create_hugeint(
        [MarshalUsing(typeof(Int128Marshaller))] Int128 input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_uhugeint(
        [MarshalUsing(typeof(UInt128Marshaller))] UInt128 input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_bignum(
        [MarshalUsing(typeof(BigIntegerMarshaller))] BigInteger input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_decimal(DuckDbDecimal input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_float(float input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_double(double input);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_interval(DuckDbInterval input);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithFree))]
    internal static partial string duckdb_value_to_string(_duckdb_value* value);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_null_value();
    
    [LibraryImport(LibraryName)]
    internal static partial _duckdb_value* duckdb_create_struct_value(_duckdb_logical_type* type, _duckdb_value** values);

    #endregion
    
    #region Appender

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_appender_create_ext(_duckdb_connection* connection,
                                                                    string? catalog,
                                                                    string? schema,
                                                                    string table,
                                                                    out _duckdb_appender* out_appender);

    [LibraryImport(LibraryName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial duckdb_state duckdb_appender_create_query(_duckdb_connection* connection,
                                                                      string query,
                                                                      idx_t column_count,
                                                                      _duckdb_logical_type** types,
                                                                      string? table_name,
                                                                      byte** column_names,
                                                                      out _duckdb_appender* out_appender);

    [LibraryImport(LibraryName)]
    internal static partial idx_t duckdb_appender_column_count(_duckdb_appender* appender);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_logical_type* duckdb_appender_column_type(_duckdb_appender* appender, idx_t col_idx);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_appender_destroy(ref _duckdb_appender* appender);

    /// <summary>
    /// Flushes and closes the appender for further appends, without deallocating it, so that
    /// <see cref="duckdb_appender_error_data" /> may still be called afterward to retrieve
    /// diagnostics for any error that occurred while flushing.
    /// </summary>
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_appender_close(_duckdb_appender* appender);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_appender_end_row(_duckdb_appender* appender);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_bool(_duckdb_appender* appender, 
                                                            [MarshalAs(UnmanagedType.I1)] bool value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_int8(_duckdb_appender* appender, sbyte value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_int16(_duckdb_appender* appender, short value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_int32(_duckdb_appender* appender, int value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_int64(_duckdb_appender* appender, long value);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_hugeint(_duckdb_appender* appender,
                                                               [MarshalUsing(typeof(Int128Marshaller))] Int128 value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_uint8(_duckdb_appender* appender, byte value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_uint16(_duckdb_appender* appender, ushort value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_uint32(_duckdb_appender* appender, uint value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_uint64(_duckdb_appender* appender, ulong value);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_uhugeint(_duckdb_appender* appender,
                                                                [MarshalUsing(typeof(UInt128Marshaller))] UInt128 value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_float(_duckdb_appender* appender, float value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_double(_duckdb_appender* appender, double value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_date(_duckdb_appender* appender, DuckDbDate value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_time(_duckdb_appender* appender, DuckDbTime value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_timestamp(_duckdb_appender* appender, DuckDbTimestamp value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_interval(_duckdb_appender* appender, DuckDbInterval value);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_varchar_length(_duckdb_appender* appender, byte* data, long length);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_blob(_duckdb_appender* appender, void *data, long length);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_null(_duckdb_appender* appender);
    
    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_default(_duckdb_appender* appender);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_value(_duckdb_appender* appender, _duckdb_value* value);

    [LibraryImport(LibraryName)]
    internal static partial _duckdb_error_data* duckdb_appender_error_data(_duckdb_appender* appender);

    [LibraryImport(LibraryName)]
    internal static partial duckdb_state duckdb_append_data_chunk(_duckdb_appender* appender, _duckdb_data_chunk* chunk);

    #endregion

    #region Error data

    [LibraryImport(LibraryName)]
    internal static partial void duckdb_destroy_error_data(ref _duckdb_error_data* error_data);

    [LibraryImport(LibraryName)]
    internal static partial DuckDbErrorKind duckdb_error_data_error_type(_duckdb_error_data* error_data);

    [LibraryImport(LibraryName)]
    [return: MarshalUsing(typeof(Utf8StringMarshallerWithoutFree))]
    internal static partial string? duckdb_error_data_message(_duckdb_error_data* error_data);

    [LibraryImport(LibraryName)]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static partial bool duckdb_error_data_has_error(_duckdb_error_data* error_data);

    #endregion
}

#pragma warning restore IDE1006, CS0169
