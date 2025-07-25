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
    /// 8-bit signed integer: TINYINT in DuckDB SQL.
    /// </summary>
    TinyInt = 2,

    /// <summary>
    /// 16-bit signed integer: SMALLINT in DuckDB SQL.
    /// </summary>
    SmallInt = 3,

    /// <summary>
    /// 32-bit signed integer: INTEGER in DuckDB SQL.
    /// </summary>
    Integer = 4,

    /// <summary>
    /// 64-bit signed integer: BIGINT in DuckDB SQL.
    /// </summary>
    BigInt = 5,

    UTinyInt = 6,  // uint8_t
    USmallInt = 7,    // uint16_t
    UInteger = 8,    // uint32_t
    UBigInt = 9,    // uint64_t

    // float
    Float = 10,
    // double
    Double = 11,

    // duckdb_timestamp (microseconds)
    Timestamp = 12,
    // duckdb_date
    Date = 13,
    // duckdb_time
    Time = 14,
    // duckdb_interval
    Interval = 15,

    // duckdb_hugeint
    HugeInt = 16,
    // duckdb_uhugeint
    UHugeInt = 32,

    // const char*
    VarChar = 17,
    // duckdb_blob
    Blob = 18,

    // duckdb_decimal
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
    
    // duckdb_hugeint
    Uuid = 27,
    
    // union type, only useful as logical type
    Union = 28,
    
    // duckdb_bit
    Bit = 29,
    // duckdb_time_tz
    TimeTz = 30,
    // duckdb_timestamp (microseconds)
    TimestampTz = 31,

    // enum type, only useful as logical type
    Any = 34,
    // duckdb_varint
    
    VarInt = 35,
    
    // enum type, only useful as logical type
    SqlNull = 36,
    // enum type, only useful as logical type
    StringLiteral = 37,
    // enum type, only useful as logical type
    IntegerLiteral = 38,
}
