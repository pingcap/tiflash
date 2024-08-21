// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/KVStore/StorageEngineType.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <TiDB/Schema/TiDB_fwd.h>
#include <tipb/schema.pb.h>

#include <optional>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <tipb/expression.pb.h>
#pragma GCC diagnostic pop

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB
{
struct SchemaNameMapper;
}

namespace TiDB
{
using DB::ColumnID;
using DB::DatabaseID;
using DB::KeyspaceID;
using DB::NullspaceID;
using DB::String;
using DB::TableID;
using DB::Timestamp;

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
    M(Num, (1 << 15))            \
    M(ParseToJSON, (1 << 18))    \
    M(IsBooleanFlag, (1 << 19))  \
    M(GeneratedColumn, (1 << 23))

enum ColumnFlag
{
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(cf, v) ColumnFlag##cf = (v),
    COLUMN_FLAGS(M)
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
    Poco::Dynamic::Var default_bit_value;
    TP tp = TypeDecimal; // TypeDecimal is not used by TiDB.
    UInt32 flag = 0;
    Int32 flen = 0;
    Int32 decimal = 0;
    Poco::Dynamic::Var charset;
    Poco::Dynamic::Var collate;
    // Elems is the element list for enum and set type.
    std::vector<std::pair<std::string, Int16>> elems;
    SchemaState state = StateNone;

#ifdef M
#error "Please undefine macro M first."
#endif
#define M(f, v)                                                    \
    inline bool has##f##Flag() const { return (flag & (v)) != 0; } \
    inline void set##f##Flag() { flag |= (v); }                    \
    inline void clear##f##Flag() { flag &= (~(v)); }
    COLUMN_FLAGS(M)
#undef M

    DB::Field defaultValueToField() const;
    CodecFlag getCodecFlag() const;
    DB::Field getDecimalValue(const String &) const;
    Int64 getEnumIndex(const String &) const;
    UInt64 getSetValue(const String &) const;
    static Int64 getTimeValue(const String &);
    static Int64 getYearValue(const String &);
    static UInt64 getBitValue(const String &);
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

    explicit PartitionDefinition(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    TableID id = DB::InvalidTableID;
    String name;
    // LessThan []string `json:"less_than"`
};

struct PartitionInfo
{
    PartitionInfo() = default;

    explicit PartitionInfo(Poco::JSON::Object::Ptr json);

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
    DatabaseID id = -1;
    KeyspaceID keyspace_id = NullspaceID;
    String name;
    String charset;
    String collate;
    SchemaState state = StatePublic;

    DBInfo() = default;
    explicit DBInfo(const String & json, KeyspaceID keyspace_id_)
    {
        deserialize(json);
        if (keyspace_id == NullspaceID)
        {
            keyspace_id = keyspace_id_;
        }
    }

    String serialize() const;

    void deserialize(const String & json_str);
};


struct TiFlashReplicaInfo
{
    UInt64 count = 0;
    std::optional<bool> available;

    /// Fields below are useless for tiflash now.
    // Strings location_labels
    // std::vector<Int64> available_partition_ids;

    Poco::JSON::Object::Ptr getJSONObject() const;
    void deserialize(Poco::JSON::Object::Ptr & json);
};

struct IndexColumnInfo
{
    IndexColumnInfo() = default;

    explicit IndexColumnInfo(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    String name;
    Int32 length = 0;
    Int32 offset = 0;
};
struct IndexInfo
{
    IndexInfo() = default;

    explicit IndexInfo(Poco::JSON::Object::Ptr json);

    Poco::JSON::Object::Ptr getJSONObject() const;

    void deserialize(Poco::JSON::Object::Ptr json);

    Int64 id = -1;
    String idx_name;
    std::vector<IndexColumnInfo> idx_cols;
    SchemaState state = StatePublic;
    Int32 index_type = -1;
    bool is_unique = false;
    bool is_primary = false;
    bool is_invisible = false;
    bool is_global = false;
};

struct TableInfo
{
    TableInfo() = default;

    TableInfo(const TableInfo &) = default;

    TableInfo & operator=(const TableInfo &) = default;

    explicit TableInfo(Poco::JSON::Object::Ptr json, KeyspaceID keyspace_id_);

    explicit TableInfo(const String & table_info_json, KeyspaceID keyspace_id_);

    String serialize() const;

    void deserialize(const String & json_str);

    void deserialize(Poco::JSON::Object::Ptr obj);

    // The meaning of this ID changed after we support TiDB partition table.
    // It is the physical table ID, i.e. table ID for non-partition table,
    // and partition ID for partition table,
    // whereas field `belonging_table_id` below actually means the table ID this partition belongs to.
    TableID id = DB::InvalidTableID;
    // The keyspace where the table belongs to.
    KeyspaceID keyspace_id = NullspaceID;
    String name;
    // Columns are listed in the order in which they appear in the schema.
    std::vector<ColumnInfo> columns;
    /// index_infos stores the index info from TiDB. But we do not store all
    /// the index infos because most of the index info is useless in TiFlash.
    /// Only the primary index info is stored now
    std::vector<IndexInfo> index_infos;
    SchemaState state = StateNone;
    bool pk_is_handle = false;
    /// when is_common_handle = true, it means this table is a clustered index table
    bool is_common_handle = false;
    Timestamp update_timestamp = 0;
    bool is_partition_table = false;
    TableID belonging_table_id = DB::InvalidTableID;
    PartitionInfo partition;
    // If the table is view, we should ignore it.
    bool is_view = false;
    // If the table is sequence, we should ignore it.
    bool is_sequence = false;
    Int64 schema_version
        = DEFAULT_UNSPECIFIED_SCHEMA_VERSION; // TODO(hyy):can be removed after removing RegionPtrWithBlock

    // The TiFlash replica info persisted by TiDB
    TiFlashReplicaInfo replica_info;

    TiDB::StorageEngine engine_type = TiDB::StorageEngine::UNSPECIFIED; // TODO(hyy):seems could be removed

    ColumnID getColumnID(const String & name) const;
    String getColumnName(ColumnID id) const;
    KeyspaceID getKeyspaceID() const;

    const ColumnInfo & getColumnInfo(ColumnID id) const;

    std::optional<std::reference_wrapper<const ColumnInfo>> getPKHandleColumn() const;

    TableInfoPtr producePartitionTableInfo(TableID table_or_partition_id, const DB::SchemaNameMapper & name_mapper)
        const;

    bool isLogicalPartitionTable() const
    {
        return is_partition_table && belonging_table_id == DB::InvalidTableID && partition.enable;
    }

    /// should not be called if is_common_handle = false.
    const IndexInfo & getPrimaryIndexInfo() const;
    size_t numColumnsInKey() const;
};


String genJsonNull();

tipb::FieldType columnInfoToFieldType(const ColumnInfo & ci);
ColumnInfo fieldTypeToColumnInfo(const tipb::FieldType & field_type);
ColumnInfo toTiDBColumnInfo(const tipb::ColumnInfo & tipb_column_info);
std::vector<ColumnInfo> toTiDBColumnInfos(
    const ::google::protobuf::RepeatedPtrField<tipb::ColumnInfo> & tipb_column_infos);

} // namespace TiDB

template <>
struct fmt::formatter<TiDB::ColumnInfo>
{
    template <typename FormatContext>
    auto format(const TiDB::ColumnInfo & ci, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(ctx.out(), "{}", ci.id);
    }
};
