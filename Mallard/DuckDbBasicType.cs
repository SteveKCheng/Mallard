namespace Mallard;

public enum DuckDbBasicType : int
{
    Invalid = 0,
    // bool
    Boolean = 1,
    // int8_t
    TinyInt = 2,
    // int16_t
    SmallInt = 3,
    // int32_t
    Integer = 4,
    // int64_t
    BigInt = 5,
    // uint8_t
    UTinyInt = 6,
    // uint16_t
    USmallInt = 7,
    // uint32_t
    UInteger = 8,
    // uint64_t
    UBigInt = 9,
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
