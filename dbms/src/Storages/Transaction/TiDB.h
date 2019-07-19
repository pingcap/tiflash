#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Transaction/Types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#pragma GCC diagnostic pop

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiDB
{

using DB::ColumnID;
using DB::DatabaseID;
using DB::Exception;
using DB::String;
using DB::TableID;
using DB::Timestamp;

// Column types.
// In format:
// TiDB type, int value, codec flag, CH type, should widen.
#ifdef M
#error "Please undefine macro M first."
#endif
#define COLUMN_TYPES(M)                              \
    M(Decimal, 0, Decimal, Decimal, false)           \
    M(Tiny, 1, VarInt, Int8, true)                   \
    M(Short, 2, VarInt, Int16, true)                 \
    M(Long, 3, VarInt, Int32, true)                  \
    M(Float, 4, Float, Float32, false)               \
    M(Double, 5, Float, Float64, false)              \
    M(Null, 6, Nil, Nothing, false)                  \
    M(Timestamp, 7, Int, DateTime, false)            \
    M(LongLong, 8, Int, Int64, false)                \
    M(Int24, 9, VarInt, Int32, true)                 \
    M(Date, 10, Int, Date, false)                    \
    M(Time, 11, Duration, Int64, false)              \
    M(Datetime, 12, Int, DateTime, false)            \
    M(Year, 13, Int, Int16, false)                   \
    M(NewDate, 14, Int, Date, false)                 \
    M(Varchar, 15, CompactBytes, String, false)      \
    M(Bit, 16, CompactBytes, UInt64, false)          \
    M(JSON, 0xf5, Json, String, false)               \
    M(NewDecimal, 0xf6, Decimal, Decimal, false)     \
    M(Enum, 0xf7, CompactBytes, Enum16, false)       \
    M(Set, 0xf8, CompactBytes, String, false)        \
    M(TinyBlob, 0xf9, CompactBytes, String, false)   \
    M(MediumBlob, 0xfa, CompactBytes, String, false) \
    M(LongBlob, 0xfb, CompactBytes, String, false)   \
    M(Blob, 0xfc, CompactBytes, String, false)       \
    M(VarString, 0xfd, CompactBytes, String, false)  \
    M(String, 0xfe, CompactBytes, String, false)     \
    M(Geometry, 0xff, CompactBytes, String, false)

enum TP
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w) Type##tt = v,
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

enum SchemaState
{
    StateNone = 0,
    StateDeleteOnly,
    StateWriteOnly,
    StateWriteReorganization,
    StateDeleteReorganization,
    StatePublic,
};

struct ColumnInfo
{
    ColumnInfo() = default;

    explicit ColumnInfo(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    ColumnID id = -1;
    String name;
    Int32 offset = -1;
    Poco::Dynamic::Var origin_default_value;
    Poco::Dynamic::Var default_value;
    TP tp = TypeDecimal; // TypeDecimal is not used by TiDB.
    UInt32 flag = 0;
    Int32 flen = 0;
    Int32 decimal = 0;
    // Elems is the element list for enum and set type.
    std::vector<std::pair<std::string, Int16>> elems;
    SchemaState state = StateNone;
    String comment;

#ifdef M
#error "Please undefine macro M first."
#endif
#define M(f, v)                                                  \
    inline bool has##f##Flag() const { return (flag & v) != 0; } \
    inline void set##f##Flag() { flag |= v; }
    COLUMN_FLAGS(M)
#undef M

    CodecFlag getCodecFlag() const;
    DB::Field defaultValueToField() const;
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

    PartitionDefinition(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    Int64 id = -1;
    String name;
    // LessThan []string `json:"less_than"`
    String comment;
};

struct PartitionInfo
{
    PartitionInfo() = default;

    PartitionInfo(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    PartitionType type = PartitionTypeRange;
    String expr;
    // Columns []CIStr       `json:"columns"`
    bool enable = false;
    std::vector<PartitionDefinition> definitions;
    UInt64 num = 0;
};

struct DBInfo
{
    Int64 id;
    String name;
    String charset;
    String collate;
    SchemaState state;

    DBInfo(const String & json) { deserialize(json); }

    void deserialize(const String & json_str);
};

struct TableInfo;
using TableInfoPtr = std::shared_ptr<TableInfo>;

struct TableInfo
{
    TableInfo() = default;

    TableInfo(const TableInfo &) = default;

    TableInfo(const String & table_info_json);

    String serialize(bool escaped) const;

    void deserialize(const String & json_str);

    DatabaseID db_id = -1;
    String db_name;
    // The meaning of this ID changed after we support TiDB partition table.
    // It is the physical table ID, i.e. table ID for non-partition table,
    // and partition ID for partition table,
    // whereas field `belonging_table_id` below actually means the table ID this partition belongs to.
    TableID id = -1;
    String name;
    // Columns are listed in the order in which they appear in the schema.
    std::vector<ColumnInfo> columns;
    SchemaState state = StateNone;
    bool pk_is_handle = false;
    String comment;
    Timestamp update_timestamp = 0;
    bool is_partition_table = false;
    TableID belonging_table_id = -1;
    PartitionInfo partition;
    Int64 schema_version = -1;

    ColumnID getColumnID(const String & name) const;

    TableInfoPtr producePartitionTableInfo(TableID table_or_partition_id) const
    {
        //
        // Some sanity checks for partition table.
        if (unlikely(!(is_partition_table && partition.enable)))
            throw Exception("Table ID " + std::to_string(id) + " seeing partition ID " + std::to_string(table_or_partition_id)
                    + " but it's not a partition table",
                DB::ErrorCodes::LOGICAL_ERROR);

        if (unlikely(std::find_if(partition.definitions.begin(), partition.definitions.end(), [table_or_partition_id](const auto & d) {
                return d.id == table_or_partition_id;
            }) == partition.definitions.end()))
            throw Exception(
                "Couldn't find partition with ID " + std::to_string(table_or_partition_id) + " in table ID " + std::to_string(id),
                DB::ErrorCodes::LOGICAL_ERROR);

        // This is a TiDB partition table, adjust the table ID by making it to physical table ID (partition ID).
        TableInfoPtr new_table = std::make_shared<TableInfo>(*this);
        new_table->belonging_table_id = id;
        new_table->id = table_or_partition_id;

        // Mangle the table name by appending partition name.
        new_table->name += "_" + std::to_string(table_or_partition_id);

        return new_table;
    }

    bool manglePartitionTableIfNeeded(TableID table_or_partition_id)
    {
        if (id == table_or_partition_id)
            // Non-partition table.
            return false;

        // Some sanity checks for partition table.
        if (unlikely(!(is_partition_table && partition.enable)))
            throw Exception("Table ID " + std::to_string(id) + " seeing partition ID " + std::to_string(table_or_partition_id)
                    + " but it's not a partition table",
                DB::ErrorCodes::LOGICAL_ERROR);

        if (unlikely(std::find_if(partition.definitions.begin(), partition.definitions.end(), [table_or_partition_id](const auto & d) {
                return d.id == table_or_partition_id;
            }) == partition.definitions.end()))
            throw Exception(
                "Couldn't find partition with ID " + std::to_string(table_or_partition_id) + " in table ID " + std::to_string(id),
                DB::ErrorCodes::LOGICAL_ERROR);

        // This is a TiDB partition table, adjust the table ID by making it to physical table ID (partition ID).
        belonging_table_id = id;
        id = table_or_partition_id;

        // Mangle the table name by appending partition name.
        name += "_" + std::to_string(table_or_partition_id);

        return true;
    }
};

using DBInfoPtr = std::shared_ptr<DBInfo>;

} // namespace TiDB
