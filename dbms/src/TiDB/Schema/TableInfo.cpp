// Copyright 2022 PingCAP, Ltd.
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
#include <TiDB/Schema/TableInfo.h>

namespace TiDB
{
TableInfo::TableInfo(Poco::JSON::Object::Ptr json)
{
    deserialize(json);
}

TableInfo::TableInfo(const String & table_info_json)
{
    deserialize(table_info_json);
}

String TableInfo::serialize() const
try
{
    std::stringstream buf;

    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("id", id);
    Poco::JSON::Object::Ptr name_json = new Poco::JSON::Object();
    name_json->set("O", name);
    name_json->set("L", name);
    json->set("name", name_json);

    Poco::JSON::Array::Ptr cols_arr = new Poco::JSON::Array();
    for (const auto & col_info : columns)
    {
        auto col_obj = col_info.getJSONObject();
        cols_arr->add(col_obj);
    }

    json->set("cols", cols_arr);
    Poco::JSON::Array::Ptr index_arr = new Poco::JSON::Array();
    for (const auto & index_info : index_infos)
    {
        auto index_info_obj = index_info.getJSONObject();
        index_arr->add(index_info_obj);
    }
    json->set("index_info", index_arr);
    json->set("state", static_cast<Int32>(state));
    json->set("pk_is_handle", pk_is_handle);
    json->set("is_common_handle", is_common_handle);
    json->set("comment", comment);
    json->set("update_timestamp", update_timestamp);
    if (is_partition_table)
    {
        json->set("belonging_table_id", belonging_table_id);
        if (belonging_table_id != DB::InvalidTableID)
        {
            json->set("is_partition_sub_table", true);
            json->set("partition", Poco::Dynamic::Var());
        }
        else
        {
            // only record partition info in LogicalPartitionTable
            json->set("partition", partition.getJSONObject());
        }
    }
    else
    {
        json->set("partition", Poco::Dynamic::Var());
    }

    json->set("schema_version", schema_version);

    json->set("tiflash_replica", replica_info.getJSONObject());

    json->stringify(buf);

    return buf.str();
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (TableInfo): " + e.displayText(),
        DB::Exception(e));
}

String JSONToString(Poco::JSON::Object::Ptr json)
{
    std::stringstream buf;
    json->stringify(buf);
    return buf.str();
}

void TableInfo::deserialize(Poco::JSON::Object::Ptr obj)
try
{
    id = obj->getValue<TableID>("id");
    name = obj->getObject("name")->getValue<String>("L");

    auto cols_arr = obj->getArray("cols");
    columns.clear();
    if (!cols_arr.isNull())
    {
        for (size_t i = 0; i < cols_arr->size(); i++)
        {
            auto col_json = cols_arr->getObject(i);
            ColumnInfo column_info(col_json);
            columns.emplace_back(column_info);
        }
    }

    auto index_arr = obj->getArray("index_info");
    index_infos.clear();
    if (!index_arr.isNull())
    {
        for (size_t i = 0; i < index_arr->size(); i++)
        {
            auto index_info_json = index_arr->getObject(i);
            IndexInfo index_info(index_info_json);
            if (index_info.is_primary)
                index_infos.emplace_back(index_info);
        }
    }

    state = static_cast<SchemaState>(obj->getValue<Int32>("state"));
    pk_is_handle = obj->getValue<bool>("pk_is_handle");
    if (obj->has("is_common_handle"))
        is_common_handle = obj->getValue<bool>("is_common_handle");
    if (!is_common_handle)
        index_infos.clear();
    comment = obj->getValue<String>("comment");
    if (obj->has("update_timestamp"))
        update_timestamp = obj->getValue<Timestamp>("update_timestamp");
    auto partition_obj = obj->getObject("partition");
    is_partition_table = obj->has("belonging_table_id") || !partition_obj.isNull();
    if (is_partition_table)
    {
        if (obj->has("belonging_table_id"))
            belonging_table_id = obj->getValue<TableID>("belonging_table_id");
        if (!partition_obj.isNull())
            partition.deserialize(partition_obj);
    }
    if (obj->has("schema_version"))
    {
        schema_version = obj->getValue<Int64>("schema_version");
    }
    if (obj->has("view") && !obj->getObject("view").isNull())
    {
        is_view = true;
    }
    if (obj->has("sequence") && !obj->getObject("sequence").isNull())
    {
        is_sequence = true;
    }
    if (obj->has("tiflash_replica"))
    {
        if (auto replica_obj = obj->getObject("tiflash_replica"); !replica_obj.isNull())
        {
            replica_info.deserialize(replica_obj);
        }
    }
    if (is_common_handle && index_infos.size() != 1)
    {
        throw DB::Exception(
            std::string(__PRETTY_FUNCTION__)
            + ": Parse TiDB schema JSON failed (TableInfo): clustered index without primary key info, json: " + JSONToString(obj));
    }
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (TableInfo): " + e.displayText() + ", json: " + JSONToString(obj),
        DB::Exception(e));
}

void TableInfo::deserialize(const String & json_str)
try
{
    if (json_str.empty())
    {
        id = DB::InvalidTableID;
        return;
    }

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json_str);

    const auto & obj = result.extract<Poco::JSON::Object::Ptr>();
    deserialize(obj);
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (TableInfo): " + e.displayText() + ", json: " + json_str,
        DB::Exception(e));
}

template <CodecFlag cf>
CodecFlag getCodecFlagBase(bool /*unsigned_flag*/)
{
    return cf;
}

template <>
CodecFlag getCodecFlagBase<CodecFlagVarInt>(bool unsigned_flag)
{
    return unsigned_flag ? CodecFlagVarUInt : CodecFlagVarInt;
}

template <>
CodecFlag getCodecFlagBase<CodecFlagInt>(bool unsigned_flag)
{
    return unsigned_flag ? CodecFlagUInt : CodecFlagInt;
}

CodecFlag ColumnInfo::getCodecFlag() const
{
    switch (tp)
    {
#ifdef M
#error "Please undefine macro M first."
#endif
#define M(tt, v, cf, ct, w) \
    case Type##tt:          \
        return getCodecFlagBase<CodecFlag##cf>(hasUnsignedFlag());
        COLUMN_TYPES(M)
#undef M
    }

    throw Exception("Unknown CodecFlag", DB::ErrorCodes::LOGICAL_ERROR);
}

ColumnID TableInfo::getColumnID(const String & name) const
{
    for (const auto & col : columns)
    {
        if (name == col.name)
        {
            return col.id;
        }
    }

    if (name == DB::MutableSupport::tidb_pk_column_name)
        return DB::TiDBPkColumnID;
    else if (name == DB::MutableSupport::version_column_name)
        return DB::VersionColumnID;
    else if (name == DB::MutableSupport::delmark_column_name)
        return DB::DelMarkColumnID;

    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Unknown column name " + name,
        DB::ErrorCodes::LOGICAL_ERROR);
}

String TableInfo::getColumnName(const ColumnID id) const
{
    for (const auto & col : columns)
    {
        if (id == col.id)
        {
            return col.name;
        }
    }

    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Invalidate column id " + std::to_string(id) + " for table " + name,
        DB::ErrorCodes::LOGICAL_ERROR);
}

const ColumnInfo & TableInfo::getColumnInfo(const ColumnID id) const
{
    for (const auto & col : columns)
    {
        if (id == col.id)
        {
            return col;
        }
    }

    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Invalidate column id " + std::to_string(id) + " for table " + name,
        DB::ErrorCodes::LOGICAL_ERROR);
}

std::optional<std::reference_wrapper<const ColumnInfo>> TableInfo::getPKHandleColumn() const
{
    if (!pk_is_handle)
        return std::nullopt;

    for (const auto & col : columns)
    {
        if (col.hasPriKeyFlag())
            return std::optional<std::reference_wrapper<const ColumnInfo>>(col);
    }

    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Cannot get handle column for table " + name,
        DB::ErrorCodes::LOGICAL_ERROR);
}

TableInfoPtr TableInfo::producePartitionTableInfo(TableID table_or_partition_id, const SchemaNameMapper & name_mapper) const
{
    // Some sanity checks for partition table.
    if (unlikely(!(is_partition_table && partition.enable)))
        throw Exception(
            "Table ID " + std::to_string(id) + " seeing partition ID " + std::to_string(table_or_partition_id)
                + " but it's not a partition table",
            DB::ErrorCodes::LOGICAL_ERROR);

    if (unlikely(std::find_if(partition.definitions.begin(), partition.definitions.end(), [table_or_partition_id](const auto & d) {
                     return d.id == table_or_partition_id;
                 })
                 == partition.definitions.end()))
        throw Exception(
            "Couldn't find partition with ID " + std::to_string(table_or_partition_id) + " in table ID " + std::to_string(id),
            DB::ErrorCodes::LOGICAL_ERROR);

    // This is a TiDB partition table, adjust the table ID by making it to physical table ID (partition ID).
    auto new_table = std::make_shared<TableInfo>();
    *new_table = *this;
    new_table->belonging_table_id = id;
    new_table->id = table_or_partition_id;

    new_table->name = name_mapper.mapPartitionName(*new_table);

    return new_table;
}
} // namespace TiDB