#include<Storages/Transaction/SchemaGetter.h>
#include<Storages/Transaction/Codec.h>
#include<tikv/Scanner.h>

namespace DB {

namespace ErrorCodes {
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
struct TxnStructure {
    constexpr static char metaPrefix[] = "m";

    static String encodeStringDataKey(const String & key) {
        std::stringstream stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeNumber<UInt64>(UInt64(StringData), stream);

        return stream.str();
    }

    static String encodeHashDataKey(const String & key, const String & field) {
        std::stringstream stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeNumber<UInt64>(UInt64(HashData), stream);
        EncodeBytes(field, stream);

        return stream.str();
    }

    static String hashDataKeyPrefix(const String & key) {
        std::stringstream stream;

        stream.write(metaPrefix, 1);

        EncodeBytes(key, stream);
        EncodeNumber<UInt64>(UInt64(HashData), stream);
        return stream.str();
    }

    static std::pair<String, String> decodeHashDataKey(const String & key) {
        if (key.rfind(metaPrefix, 0) != 0) {
            throw Exception("invalid encoded hash data key prefix.", ErrorCodes::SCHEMA_SYNC_ERROR);
        }


        size_t idx = 0;
        idx += 1;
        String decode_key = DecodeBytes(idx, key);

        UInt64 tp = DecodeInt<UInt64>(idx, key);
        if (char(tp) != HashData) {
            throw Exception("invalid encoded hash data key flag:" + std::to_string(tp), ErrorCodes::SCHEMA_SYNC_ERROR);
        }

        String field = DecodeBytes(idx, key);
        return make_pair(decode_key, field);
    }

    // only for debug
    static String StringToHex(const std::string& input)
    {
        static const char* const lut = "0123456789ABCDEF";
        size_t len = input.length();

        std::string output;
        output.reserve(2 * len);
        for (size_t i = 0; i < len; ++i)
        {
            const unsigned char c = input[i];
            output.push_back(lut[c >> 4]);
            output.push_back(lut[c & 15]);
        }
        return output;
    }

public:
    static String Get(pingcap::kv::Snapshot & snap, const String & key)
    {
        String encode_key = encodeStringDataKey(key);
        String value = snap.Get(encode_key);
        return value;
    }

    static String HGet(pingcap::kv::Snapshot & snap, const String & key, const String & field)
    {
        String encode_key = encodeHashDataKey(key, field);
        String value = snap.Get(encode_key);
        return value;
    }

    // For convinient, we only return values.
    static std::vector<std::pair<String, String>> HGetAll(pingcap::kv::Snapshot & snap, const String & key) {
        auto tikv_key_prefix = hashDataKeyPrefix(key) ;
        String tikv_key_end = pingcap::kv::prefixNext(tikv_key_prefix);
        auto scanner = snap.Scan(tikv_key_prefix, tikv_key_end);
        std::vector<std::pair<String, String>> res;
        while(scanner.valid) {
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

void SchemaDiff::deserialize(const String & data) {
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(data);
    auto obj = result.extract<Poco::JSON::Object::Ptr>();
    version = obj->getValue<Int64>("version");
    type = (SchemaActionType)obj->getValue<Int32>("type");
    schema_id = obj->getValue<Int64>("schema_id");
    table_id = obj->getValue<Int64>("table_id");

    old_table_id = obj->getValue<Int64>("old_table_id");
    old_schema_id = obj->getValue<Int64>("old_schema_id");
}

Int64 SchemaGetter::getVersion() {
    String ver = TxnStructure::Get(snap, schemaVersionKey);
    if (ver == "")
        return 0;
    return std::stoll(ver);
}

String SchemaGetter::getSchemaDiffKey(Int64 ver) {
    return std::string(schemaDiffPrefix) + ":" + std::to_string(ver);
}

SchemaDiff SchemaGetter::getSchemaDiff(Int64 ver) {
    String key = getSchemaDiffKey(ver);
    String data = TxnStructure::Get(snap, key);
    if (data == "") {
        throw Exception("cannot find schema diff for version: " + std::to_string(ver), ErrorCodes::SCHEMA_SYNC_ERROR);
    }
    SchemaDiff diff;
    diff.deserialize(data);
    return diff;
}

String SchemaGetter::getDBKey(DatabaseID db_id) {
    return String(DBPrefix) + ":" + std::to_string(db_id);
}

String SchemaGetter::getTableKey(TableID table_id) {
    return String(TablePrefix) + ":" + std::to_string(table_id);
}

TiDB::DBInfoPtr SchemaGetter::getDatabase(DatabaseID db_id) {
    String key = getDBKey(db_id);
    String json = TxnStructure::HGet(snap, DBs, key);

    if (json == "")
        return nullptr;

    auto db_info = std::make_shared<TiDB::DBInfo>(json);
    return db_info;
}

TiDB::TableInfoPtr SchemaGetter::getTableInfo(DatabaseID db_id, TableID table_id) {
    String db_key = getDBKey(db_id);
    if (!checkDBExists(db_key)) {
        throw Exception();
    }
    String table_key = getTableKey(table_id);
    String table_info_json = TxnStructure::HGet(snap, db_key, table_key);
    if (table_info_json == "")
        return nullptr;
    TiDB::TableInfoPtr table_info  = std::make_shared<TiDB::TableInfo>(table_info_json, false);
    return table_info;
}

std::vector<TiDB::DBInfoPtr> SchemaGetter::listDBs() {
    std::vector<TiDB::DBInfoPtr> res;
    auto pairs = TxnStructure::HGetAll(snap, DBs);
    for (auto pair: pairs) {
        auto db_info = std::make_shared<TiDB::DBInfo>(pair.second);
        res.push_back(db_info);
    }
    return res;
}

bool SchemaGetter::checkDBExists(const String & key) {
    String value = TxnStructure::HGet(snap, DBs, key);
    return value.size() > 0;
}

std::vector<TiDB::TableInfoPtr> SchemaGetter::listTables(DatabaseID db_id) {
    auto db_key = getDBKey(db_id);
    if (!checkDBExists(db_key)) {
        throw Exception("DB Not Exists!", ErrorCodes::SCHEMA_SYNC_ERROR);
    }

    std::vector<TiDB::TableInfoPtr> res;

    auto kv_pairs = TxnStructure::HGetAll(snap, db_key);

    for (auto kv_pair : kv_pairs) {
        const String & key = kv_pair.first;
        if (key.rfind(TablePrefix, 0) != 0) {
            continue;
        }
        const String & json = kv_pair.second;
        auto table_info = std::make_shared<TiDB::TableInfo>(json, false);

        res.push_back(table_info);
    }
    return res;

}

// end of namespace.
}
