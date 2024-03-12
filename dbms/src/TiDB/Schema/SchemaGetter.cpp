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
#include <Storages/Transaction/DatumCodec.h>
#include <TiDB/Schema/SchemaGetter.h>
#include <pingcap/kv/Scanner.h>

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
        EncodeUInt<UInt64>(UInt64(StringData), stream);

        return stream.releaseStr();
    }

    static String encodeHashDataKey(const String & key, const String & field)
    {
        WriteBufferFromOwnString stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeUInt<UInt64>(UInt64(HashData), stream);
        EncodeBytes(field, stream);

        return stream.releaseStr();
    }

    static String hashDataKeyPrefix(const String & key)
    {
        WriteBufferFromOwnString stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeUInt<UInt64>(UInt64(HashData), stream);
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
        if (char(tp) != HashData)
        {
            throw TiFlashException("invalid encoded hash data key flag:" + std::to_string(tp), Errors::Table::SyncError);
        }

        String field = DecodeBytes(idx, key);
        return make_pair(decode_key, field);
    }

public:
    static String get(pingcap::kv::Snapshot & snap, const String & key)
    {
        String encode_key = encodeStringDataKey(key);
        String value = snap.Get(encode_key);
        return value;
    }

    static String hGet(pingcap::kv::Snapshot & snap, const String & key, const String & field)
    {
        String encode_key = encodeHashDataKey(key, field);
        String value = snap.Get(encode_key);
        return value;
    }

    // For convinient, we only return values.
    static std::vector<std::pair<String, String>> hGetAll(pingcap::kv::Snapshot & snap, const String & key)
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
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(data);
    auto obj = result.extract<Poco::JSON::Object::Ptr>();
    version = obj->getValue<Int64>("version");
    type = static_cast<SchemaActionType>(obj->getValue<Int32>("type"));
    schema_id = obj->getValue<Int64>("schema_id");
    table_id = obj->getValue<Int64>("table_id");

    old_table_id = obj->getValue<Int64>("old_table_id");
    old_schema_id = obj->getValue<Int64>("old_schema_id");

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

Int64 SchemaGetter::getVersion()
{
    String ver = TxnStructure::get(snap, schemaVersionKey);
    if (ver.empty())
        return 0;
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
        LOG_WARNING(log, "The schema diff for version {}, key {} is empty.", ver, key);
        return std::nullopt;
    }
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
    return std::make_shared<TiDB::DBInfo>(json);
}

TiDB::TableInfoPtr SchemaGetter::getTableInfo(DatabaseID db_id, TableID table_id)
{
    String db_key = getDBKey(db_id);
    if (!checkDBExists(db_key))
    {
        throw Exception();
    }
    String table_key = getTableKey(table_id);
    String table_info_json = TxnStructure::hGet(snap, db_key, table_key);
    if (table_info_json.empty())
        return nullptr;
    LOG_DEBUG(log, "Get TableInfo from TiKV, table_id={} {}", table_id, table_info_json);
    return std::make_shared<TiDB::TableInfo>(table_info_json, keyspace_id);
}

std::vector<TiDB::DBInfoPtr> SchemaGetter::listDBs()
{
    std::vector<TiDB::DBInfoPtr> res;
    auto pairs = TxnStructure::hGetAll(snap, DBs);
    for (const auto & pair : pairs)
    {
        auto db_info = std::make_shared<TiDB::DBInfo>(pair.second);
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
        throw TiFlashException(fmt::format("The database does not exist, database_id={}", db_id), Errors::Table::SyncError);
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
        auto table_info = std::make_shared<TiDB::TableInfo>(json);

        res.push_back(table_info);
    }
    return res;
}

} // namespace DB
