#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Transaction/Types.h>
#include <common/JSON.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace TiDB
{

using DB::String;
using DB::DatabaseID;
using DB::TableID;
using DB::ColumnID;
using DB::Exception;

// Column types.
// In format:
// TiDB type, int value, signed codec flag, unsigned codec flag, signed CH type, unsigned CH type.
#ifdef M
#error "Please undefine macro M first."
#endif
#define COLUMN_TYPES(M)                                             \
    M(Decimal, 0, Decimal, Decimal, Decimal, Decimal)               \
    M(Tiny, 1, VarInt, VarUInt, Int8, UInt8)                        \
    M(Short, 2, VarInt, VarUInt, Int16, UInt16)                     \
    M(Long, 3, VarInt, VarUInt, Int32, UInt32)                      \
    M(Float, 4, Float, Float, Float32, Float32)                     \
    M(Double, 5, Float, Float, Float64, Float64)                    \
    M(Null, 6, Nil, Nil, Nothing, Nothing)                          \
    M(Timestamp, 7, Int, Int, DateTime, DateTime)                   \
    M(Longlong, 8, Int, UInt, Int64, UInt64)                        \
    M(Int24, 9, VarInt, VarUInt, Int32, UInt32)                     \
    M(Date, 10, Int, Int, Date, Date)                               \
    M(Time, 11, Duration, Duration, Int64, Int64)                   \
    M(Datetime, 12, Int, Int, DateTime, DateTime)                   \
    M(Year, 13, Int, Int, Int16, Int16)                             \
    M(NewDate, 14, Int, Int, Date, Date)                            \
    M(Varchar, 15, CompactBytes, CompactBytes, String, String)      \
    M(Bit, 16, CompactBytes, CompactBytes, UInt64, UInt64)          \
    M(JSON, 0xf5, Json, Json, String, String)                       \
    M(NewDecimal, 0xf6, Decimal, Decimal, Decimal, Decimal)         \
    M(Enum, 0xf7, CompactBytes, CompactBytes, Enum16, Enum16)       \
    M(Set, 0xf8, CompactBytes, CompactBytes, String, String)        \
    M(TinyBlob, 0xf9, CompactBytes, CompactBytes, String, String)   \
    M(MediumBlob, 0xfa, CompactBytes, CompactBytes, String, String) \
    M(LongBlob, 0xfb, CompactBytes, CompactBytes, String, String)   \
    M(Blob, 0xfc, CompactBytes, CompactBytes, String, String)       \
    M(VarString, 0xfd, CompactBytes, CompactBytes, String, String)  \
    M(String, 0xfe, CompactBytes, CompactBytes, String, String)     \
    M(Geometry, 0xff, CompactBytes, CompactBytes, String, String)


enum TP
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, cfu, ct, ctu) Type##tt = v,
    COLUMN_TYPES(M)
#undef M
};


// Column flags.
#ifdef M
#error "Please undefine macro M first."
#endif
#define COLUMN_FLAGS(M)          \
    M(NotNull, (1 << 0))         \
    M(PriKey, (1 << 1))          \
    M(UniqueKey, (1 << 2))       \
    M(MultipleKey, (1 << 3))     \
    M(Blob, (1 << 4))            \
    M(Unsigned, (1 << 5))        \
    M(Zerofill, (1 << 6))        \
    M(Binary, (1 << 7))          \
    M(Enum, (1 << 8))            \
    M(AutoIncrement, (1 << 9))   \
    M(Timestamp, (1 << 10))      \
    M(Set, (1 << 11))            \
    M(NoDefaultValue, (1 << 12)) \
    M(OnUpdateNow, (1 << 13))    \
    M(PartKey, (1 << 14))        \
    M(Num, (1 << 15))


// Codec flags.
// In format: TiDB codec flag, int value.
#ifdef M
#error "Please undefine macro M first."
#endif
#define CODEC_FLAGS(M) \
    M(Nil, 0)          \
    M(Bytes, 1)        \
    M(CompactBytes, 2) \
    M(Int, 3)          \
    M(UInt, 4)         \
    M(Float, 5)        \
    M(Decimal, 6)      \
    M(Duration, 7)     \
    M(VarInt, 8)       \
    M(VarUInt, 9)      \
    M(Json, 10)        \
    M(Max, 250)


enum CodecFlag
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(cf, v) CodecFlag##cf = v,
    CODEC_FLAGS(M)
#undef M
};


struct ColumnInfo
{
    ColumnInfo() = default;

    explicit ColumnInfo(const JSON & json);

    String serialize() const;

    void deserialize(const JSON & json);

    ColumnID id = -1;
    String name;
    Int32 offset = -1;
    String origin_default_value;
    bool has_origin_default_value = false;
    String default_value;
    bool has_default_value = false;
    TP tp = TypeDecimal; // TypeDecimal is not used by TiDB.
    UInt32 flag = 0;
    Int32 flen = 0;
    Int32 decimal = 0;
    // Elems is the element list for enum and set type.
    std::vector<std::pair<std::string, Int16>> elems;
    UInt8 state = 0;
    String comment;

#ifdef M
#error "Please undefine macro M first."
#endif
#define M(f, v) \
    inline bool has##f##Flag() const { return (flag & v) != 0; } \
    inline void set##f##Flag() { flag |= v; }
    COLUMN_FLAGS(M)
#undef M

    inline CodecFlag getCodecFlag() const
    {
        switch (tp)
        {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, cfu, ct, ctu) \
    case Type##tt:                 \
        return hasUnsignedFlag() ? CodecFlag##cfu : CodecFlag##cf;
            COLUMN_TYPES(M)
#undef M
        }

        throw Exception("Unknown CodecFlag", DB::ErrorCodes::LOGICAL_ERROR);
    }
};

enum PartitionType
{
    PartitionTypeRange = 1,
    PartitionTypeHash = 2,
    PartitionTypeList = 3,
};

struct PartitionDefinition
{
    PartitionDefinition() = default;

    PartitionDefinition(const JSON & json);

    String serialize() const;

    void deserialize(const JSON & json);

    Int64 id = -1;
    String name;
    // LessThan []string `json:"less_than"`
    String comment;
};

struct PartitionInfo
{
    PartitionInfo() = default;

    PartitionInfo(const JSON & json);

    String serialize() const;

    void deserialize(const JSON & json);

    PartitionType type = PartitionTypeRange;
    String expr;
    // Columns []CIStr       `json:"columns"`
    bool enable = false;
    std::vector<PartitionDefinition> definitions;
    UInt64 num = 0;
};

struct TableInfo
{
    TableInfo() = default;

    TableInfo(const String & table_info_json, bool escaped);

    String serialize(bool escaped) const;

    void deserialize(const String & json_str, bool escaped);

    DatabaseID db_id = -1;
    String db_name;
    // The meaning of this ID changed after we support TiDB partition ID.
    // It is the physical table ID, i.e. table ID for non-partition table,
    // and partition ID for partition table
    // whereas field `belonging_table_id` actual means the table ID this partition belongs to.
    TableID id = -1;
    String name;
    // Columns are listed in the order in which they appear in the schema.
    std::vector<ColumnInfo> columns;
    UInt8 state = 0;
    bool pk_is_handle = false;
    String comment;
    bool is_partition_table = false;
    TableID belonging_table_id = -1;
    PartitionInfo partition;
    Int64 schema_version = -1;

    ColumnID getColumnID(const String & name) const
    {
        for (auto col : columns)
        {
            if (name == col.name)
            {
                return col.id;
            }
        }
        // TODO: use DB::MutableSupport::tidb_pk_column_name instead
        if (name == "_tidb_rowid")
        {
            // TODO: use XXXColumnID
            return -1;
        }
        throw Exception("unknown column name " + name, DB::ErrorCodes::LOGICAL_ERROR);
    }

    bool manglePartitionTableIfNeeded(TableID table_or_partition_id)
    {
        if (id == table_or_partition_id)
            // Non-partition table.
            return false;

        // Some sanity checks for partition table.
        if (unlikely(!(is_partition_table && partition.enable)))
            throw Exception("Table ID " + std::to_string(id) + " seeing partition ID " + std::to_string(table_or_partition_id) + " but it's not a partition table", DB::ErrorCodes::LOGICAL_ERROR);

        if (unlikely(std::find_if(partition.definitions.begin(), partition.definitions.end(), [table_or_partition_id](const auto & d) { return d.id == table_or_partition_id; }) == partition.definitions.end()))
            throw Exception("Couldn't find partition with ID " + std::to_string(table_or_partition_id) + " in table ID " + std::to_string(id), DB::ErrorCodes::LOGICAL_ERROR);

        // This is a TiDB partition table, adjust the table ID by making it to physical table ID (partition ID).
        belonging_table_id = id;
        id = table_or_partition_id;

        // Mangle the table name by appending partition name.
        name += "_" + std::to_string(table_or_partition_id);

        return true;
    }
};

} // namespace TiDB
