namespace Mallard;

/// <summary>
/// The kinds of values that can be read from or stored into DuckDB.
/// </summary>
/// <remarks>
/// <para>
/// The word "type" is deliberately avoided for the name of this enumeration 
/// to avoid confusion with DuckDB's logical types, or .NET types.
/// (Nevertheless, in DuckDB's C API, this enumeration is called <c>duckdb_type</c>.)
/// </para>
/// <para>
/// This enumeration mostly specifies the physical representation of values
/// but not completely.  For instance, decimal numbers in DuckDB vectors
/// are stored as integers with a width <c>w</c>, and number of decimal
/// digits for the fractional part of the number <c>d</c>, that
/// is specified when the type of the vector is defined as holding elements
/// of (logical) type <c>DECIMAL(w,d)</c>.  That supplementary information
/// is queried separately.
/// </para>
/// </remarks>
public enum DuckDbValueKind : int
{
    Invalid = 0,

    /// <summary>
    /// Boolean values; BOOLEAN in DuckDB SQL.
    /// </summary>
    Boolean = 1,

    /// <summary>
    /// 8-bit signed integer: TINYINT (INT1) in DuckDB SQL.
    /// </summary>
    TinyInt = 2,

    /// <summary>
    /// 16-bit signed integer: SMALLINT (INT2, INT16, SHORT) in DuckDB SQL.
    /// </summary>
    SmallInt = 3,

    /// <summary>
    /// 32-bit signed integer: INTEGER (INT4, INT32, INT, SIGNED) in DuckDB SQL.
    /// </summary>
    Integer = 4,

    /// <summary>
    /// 64-bit signed integer: BIGINT (INT8, INT64, LONG) in DuckDB SQL.
    /// </summary>
    BigInt = 5,

    /// <summary>
    /// 8-bit unsigned integer: UTINYINT (UINT8) in DuckDB SQL.
    /// </summary>
    UTinyInt = 6,
    
    /// <summary>
    /// 16-bit unsigned integer: USMALLINT (UINT16) in DuckDB SQL.
    /// </summary>
    USmallInt = 7,
    
    /// <summary>
    /// 32-bit unsigned integer: UINTEGER (UINT32) in DuckDB SQL.
    /// </summary>
    UInteger = 8,
    
    /// <summary>
    /// 64-bit unsigned integer: UBIGINT (UINT64) in DuckDB SQL.
    /// </summary>
    UBigInt = 9,    // uint64_t

    /// <summary>
    /// Single-precision (32-bit) floating-point number: FLOAT (FLOAT4, REAL) in DuckDB SQL. 
    /// </summary>
    Float = 10,
    
    /// <summary>
    /// Double-precision (64-bit) floating-point number: FLOAT (FLOAT8) in DuckDB SQL.
    /// </summary>
    Double = 11,

    // duckdb_timestamp (microseconds)
    Timestamp = 12,
    // duckdb_date
    Date = 13,
    // duckdb_time
    Time = 14,
    // duckdb_interval
    Interval = 15,

    /// <summary>
    /// 128-bit signed integer: HUGEINT (INT128) in DuckDB SQL.
    /// </summary>
    HugeInt = 16,

    /// <summary>
    /// 128-bit unsigned integer: UHUGEINT (UINT128) in DuckDB SQL.
    /// </summary>
    UHugeInt = 32,

    /// <summary>
    /// Text string: VARCHAR (CHAR, BPCHAR, STRING, TEXT) in DuckDB SQL.
    /// </summary>
    VarChar = 17,
    
    /// <summary>
    /// Blob (Binary Large Object): BLOB (BYTEA, BINARY, VARBINARY) in DuckDB SQL.
    /// </summary>
    Blob = 18,

    /// <summary>
    /// Fixed-point decimal numbers: DECIMAL (NUMERIC) in DuckDB SQL.
    /// </summary>
    Decimal = 19,

    // duckdb_timestamp_s (seconds)
    TimestampSeconds = 20,
    // duckdb_timestamp_ms (milliseconds)
    TimestampMilliseconds = 21,
    // duckdb_timestamp_ns (nanoseconds)
    TimestampNanoseconds = 22,
    
    // enum type, only useful as logical type
    Enum = 23,
    // list type, only useful as logical type
    List = 24,
    // struct type, only useful as logical type
    Struct = 25,
    // map type, only useful as logical type
    Map = 26,
    
    // duckdb_array, only useful as logical type
    Array = 33,
    
    /// <summary>
    /// 128-bit UUID (Universally Unique Identifiers): UUID in DuckDB SQL.
    /// </summary>
    Uuid = 27,
    
    // union type, only useful as logical type
    Union = 28,

    /// <summary>
    /// Bit-strings: BITSTRING (BIT) in DuckDB SQL.
    /// </summary>
    Bit = 29,
    
    // duckdb_time_tz
    TimeTz = 30,
    // duckdb_timestamp (microseconds)
    TimestampTz = 31,

    // enum type, only useful as logical type
    Any = 34,
    
    /// <summary>
    /// Variable-length integers: BIGNUM in DuckDB SQL.
    /// </summary>
    VarInt = 35,
    
    // enum type, only useful as logical type
    SqlNull = 36,
    // enum type, only useful as logical type
    StringLiteral = 37,
    // enum type, only useful as logical type
    IntegerLiteral = 38,
}
