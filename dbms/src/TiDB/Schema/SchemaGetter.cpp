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

#include <Common/TiFlashException.h>
#include <Storages/KVStore/TiKVHelpers/KeyspaceSnapshot.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Schema/SchemaGetter.h>
#include <common/logger_useful.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int SCHEMA_SYNC_ERROR;
}

constexpr char schemaVersionKey[] = "SchemaVersionKey";

constexpr char schemaDiffPrefix[] = "Diff";

constexpr char DBPrefix[] = "DB";

constexpr char DBs[] = "DBs";

constexpr char TablePrefix[] = "Table";

constexpr char StringData = 's';

constexpr char HashData = 'h';

// TODO:: Refine Encode Process;
struct TxnStructure
{
    constexpr static char metaPrefix[] = "m";

    static String encodeStringDataKey(const String & key)
    {
        WriteBufferFromOwnString stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeUInt<UInt64>(static_cast<UInt64>(StringData), stream);

        return stream.releaseStr();
    }

    static String encodeHashDataKey(const String & key, const String & field)
    {
        WriteBufferFromOwnString stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeUInt<UInt64>(static_cast<UInt64>(HashData), stream);
        EncodeBytes(field, stream);

        return stream.releaseStr();
    }

    static String hashDataKeyPrefix(const String & key)
    {
        WriteBufferFromOwnString stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeUInt<UInt64>(static_cast<UInt64>(HashData), stream);
        return stream.releaseStr();
    }

    static std::pair<String, String> decodeHashDataKey(const String & key)
    {
        if (key.rfind(metaPrefix, 0) != 0)
        {
            throw TiFlashException("invalid encoded hash data key prefix.", Errors::Table::SyncError);
        }


        size_t idx = 0;
        idx += 1;
        String decode_key = DecodeBytes(idx, key);

        UInt64 tp = DecodeUInt<UInt64>(idx, key);
        if (static_cast<char>(tp) != HashData)
        {
            throw TiFlashException(Errors::Table::SyncError, "invalid encoded hash data key flag: {}", tp);
        }

        String field = DecodeBytes(idx, key);
        return make_pair(decode_key, field);
    }

public:
    static String get(KeyspaceSnapshot & snap, const String & key)
    {
        String encode_key = encodeStringDataKey(key);
        String value = snap.Get(encode_key);
        return value;
    }

    static String hGet(KeyspaceSnapshot & snap, const String & key, const String & field)
    {
        String encode_key = encodeHashDataKey(key, field);
        return snap.Get(encode_key);
    }

    static String mvccGet(KeyspaceSnapshot & snap, const String & key, const String & field)
    {
        auto encode_key = encodeHashDataKey(key, field);
        auto mvcc_info = snap.mvccGet(encode_key);
        auto values = mvcc_info.values();
        if (values.empty())
        {
            return "";
        }

        String target_value;
        uint64_t max_ts = 0;
        for (const auto & value_pair : values)
        {
            auto ts = value_pair.start_ts();
            if (max_ts == 0 || ts > max_ts)
            {
                target_value = value_pair.value();
                max_ts = ts;
            }
        }

        return target_value;
    }

    // For convinient, we only return values.
    static std::vector<std::pair<String, String>> hGetAll(KeyspaceSnapshot & snap, const String & key)
    {
        auto tikv_key_prefix = hashDataKeyPrefix(key);
        String tikv_key_end = pingcap::kv::prefixNext(tikv_key_prefix);
        auto scanner = snap.Scan(tikv_key_prefix, tikv_key_end);
        std::vector<std::pair<String, String>> res;
        while (scanner.valid)
        {
            String raw_key = scanner.key();
            auto pair = decodeHashDataKey(raw_key);
            auto field = pair.second;
            String value = scanner.value();
            res.push_back(std::make_pair(field, value));
            scanner.next();
        }
        return res;
    }
};

AffectedOption::AffectedOption(Poco::JSON::Object::Ptr json)
    : schema_id(0)
    , table_id(0)
    , old_table_id(0)
    , old_schema_id(0)
{
    deserialize(json);
}

void AffectedOption::deserialize(Poco::JSON::Object::Ptr json)
{
    schema_id = json->getValue<Int64>("schema_id");
    table_id = json->getValue<Int64>("table_id");
    old_table_id = json->getValue<Int64>("old_table_id");
    old_schema_id = json->getValue<Int64>("old_schema_id");
}

void SchemaDiff::deserialize(const String & data)
{
    assert(!data.empty()); // should be skipped by upper level logic
    Poco::JSON::Parser parser;
    try
    {
        Poco::Dynamic::Var result = parser.parse(data);
        if (result.isEmpty())
        {
            throw Exception("The schema diff deserialize failed " + data);
        }
        auto obj = result.extract<Poco::JSON::Object::Ptr>();
        version = obj->getValue<Int64>("version");
        type = static_cast<SchemaActionType>(obj->getValue<Int32>("type"));
        schema_id = obj->getValue<Int64>("schema_id");
        table_id = obj->getValue<Int64>("table_id");

        old_table_id = obj->getValue<Int64>("old_table_id");
        old_schema_id = obj->getValue<Int64>("old_schema_id");

        if (obj->has("regenerate_schema_map"))
        {
            regenerate_schema_map = obj->getValue<bool>("regenerate_schema_map");
        }

        affected_opts.clear();
        auto affected_arr = obj->getArray("affected_options");
        if (!affected_arr.isNull())
        {
            for (size_t i = 0; i < affected_arr->size(); i++)
            {
                auto affected_opt_json = affected_arr->getObject(i);
                AffectedOption affected_option(affected_opt_json);
                affected_opts.emplace_back(affected_option);
            }
        }
    }
    catch (...)
    {
        LOG_WARNING(Logger::get(), "failed to deserialize {}", data);
        throw;
    }
}

Int64 SchemaGetter::getVersion()
{
    String ver = TxnStructure::get(snap, schemaVersionKey);
    if (ver.empty())
        return SchemaVersionNotExist;
    return std::stoll(ver);
}

bool SchemaGetter::checkSchemaDiffExists(Int64 ver)
{
    String key = getSchemaDiffKey(ver);
    String data = TxnStructure::get(snap, key);
    return !data.empty();
}

String SchemaGetter::getSchemaDiffKey(Int64 ver)
{
    return std::string(schemaDiffPrefix) + ":" + std::to_string(ver);
}

std::optional<SchemaDiff> SchemaGetter::getSchemaDiff(Int64 ver)
{
    String key = getSchemaDiffKey(ver);
    String data = TxnStructure::get(snap, key);
    if (data.empty())
    {
        LOG_WARNING(log, "The schema diff is empty, schema_version={} key={}", ver, key);
        return std::nullopt;
    }
    LOG_TRACE(log, "Get SchemaDiff from TiKV, schema_version={} data={}", ver, data);
    SchemaDiff diff;
    diff.deserialize(data);
    return diff;
}

String SchemaGetter::getDBKey(DatabaseID db_id)
{
    return String(DBPrefix) + ":" + std::to_string(db_id);
}

String SchemaGetter::getTableKey(TableID table_id)
{
    return String(TablePrefix) + ":" + std::to_string(table_id);
}

TiDB::DBInfoPtr SchemaGetter::getDatabase(DatabaseID db_id)
{
    String key = getDBKey(db_id);
    String json = TxnStructure::hGet(snap, DBs, key);
    if (json.empty())
        return nullptr;

    LOG_DEBUG(log, "Get DatabaseInfo from TiKV, database_id={} {}", db_id, json);
    return std::make_shared<TiDB::DBInfo>(json, keyspace_id);
}

template <bool mvcc_get>
std::pair<TiDB::TableInfoPtr, bool> SchemaGetter::getTableInfoImpl(DatabaseID db_id, TableID table_id)
{
    String db_key = getDBKey(db_id);
    // Note: Do not check the existence of `db_key` here, otherwise we can not
    //       get the table info after database is dropped.
    String table_key = getTableKey(table_id);
    String table_info_json = TxnStructure::hGet(snap, db_key, table_key);
    bool get_by_mvcc = false;
    if (table_info_json.empty())
    {
        if constexpr (!mvcc_get)
        {
            return {nullptr, false};
        }

        LOG_WARNING(log, "The table is dropped in TiKV, try to get the latest table_info, table_id={}", table_id);
        table_info_json = TxnStructure::mvccGet(snap, db_key, table_key);
        get_by_mvcc = true;
        if (table_info_json.empty())
        {
            LOG_ERROR(
                log,
                "The table is dropped in TiKV, and the latest table_info is still empty, it should be GCed, "
                "table_id={}",
                table_id);
            return {nullptr, get_by_mvcc};
        }
    }
    LOG_DEBUG(log, "Get TableInfo from TiKV, table_id={} {}", table_id, table_info_json);
    return {std::make_shared<TiDB::TableInfo>(table_info_json, keyspace_id), get_by_mvcc};
}
template std::pair<TiDB::TableInfoPtr, bool> SchemaGetter::getTableInfoImpl<false>(DatabaseID db_id, TableID table_id);
template std::pair<TiDB::TableInfoPtr, bool> SchemaGetter::getTableInfoImpl<true>(DatabaseID db_id, TableID table_id);

std::vector<TiDB::DBInfoPtr> SchemaGetter::listDBs()
{
    std::vector<TiDB::DBInfoPtr> res;
    auto pairs = TxnStructure::hGetAll(snap, DBs);
    for (const auto & pair : pairs)
    {
        auto db_info = std::make_shared<TiDB::DBInfo>(pair.second, keyspace_id);
        res.push_back(db_info);
    }
    return res;
}

bool SchemaGetter::checkDBExists(const String & key)
{
    String value = TxnStructure::hGet(snap, DBs, key);
    return !value.empty();
}

std::vector<TiDB::TableInfoPtr> SchemaGetter::listTables(DatabaseID db_id)
{
    auto db_key = getDBKey(db_id);
    if (!checkDBExists(db_key))
    {
        LOG_ERROR(log, "The database does not exist, database_id={}", db_id);
        return {};
    }

    std::vector<TiDB::TableInfoPtr> res;

    auto kv_pairs = TxnStructure::hGetAll(snap, db_key);

    for (const auto & kv_pair : kv_pairs)
    {
        const String & key = kv_pair.first;
        if (key.rfind(TablePrefix, 0) != 0)
        {
            continue;
        }
        const String & json = kv_pair.second;
        auto table_info = std::make_shared<TiDB::TableInfo>(json, keyspace_id);

        res.push_back(table_info);
    }
    return res;
}

} // namespace DB
