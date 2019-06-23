#include <IO/ReadBufferFromString.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/TiDB.h>

namespace JsonSer
{

using DB::WriteBuffer;

template <typename T = bool>
void serValue(WriteBuffer & buf, const bool & b)
{
    writeString(b ? "true" : "false", buf);
}

template <typename T>
typename std::enable_if_t<std::is_integral<T>::value || std::is_enum<T>::value> serValue(WriteBuffer & buf, T i)
{
    writeIntText(static_cast<Int64>(i), buf);
}

template <typename T>
typename std::enable_if_t<std::is_floating_point<T>::value> serValue(WriteBuffer & buf, T f)
{
    writeFloadText(f, buf);
}

// String that has been already encoded as JSON.
struct JsonString : public std::string
{
};

template <typename T = JsonString>
void serValue(WriteBuffer & buf, const JsonString & qs)
{
    writeString(qs, buf);
}

template <typename T = std::string>
void serValue(WriteBuffer & buf, const std::string & s)
{
    writeJSONString(s, buf);
}

template <typename T>
void serValue(WriteBuffer & buf, const std::vector<T> & v)
{
    writeString("[", buf);
    bool first = true;
    for (auto & e : v)
    {
        first = first ? false : (writeString(",", buf), false);
        serValue(buf, e);
    }
    writeString("]", buf);
}

template <typename T = std::function<void(WriteBuffer &)>>
void serValue(WriteBuffer & buf, const std::function<void(WriteBuffer &)> & s)
{
    s(buf);
}

template <typename T>
std::function<void(WriteBuffer &)> Nullable(const T & value, bool is_null)
{
    return [value, is_null](WriteBuffer & buf) { is_null ? writeString("null", buf) : serValue(buf, value); };
}

template <typename T>
struct Field
{
    Field(std::string name_, T value_, bool skip_ = false) : name(std::move(name_)), value(std::move(value_)), skip(skip_) {}
    std::string name;
    T value;
    bool skip;
};

template <typename T>
void serField(WriteBuffer & buf, const Field<T> & field)
{
    writeJSONString(field.name, buf);
    writeString(":", buf);
    serValue(buf, field.value);
}

template <typename T>
void serFields(WriteBuffer & buf, const T & last)
{
    if (!last.skip)
        serField(buf, last);
}

template <typename T, typename... Rest>
void serFields(WriteBuffer & buf, const T & first, const Rest &... rest)
{
    if (!first.skip)
    {
        serField(buf, first);
        writeString(",", buf);
    }
    serFields(buf, rest...);
}

template <typename... T>
void serValue(WriteBuffer & buf, const T &... fields)
{
    writeString("{", buf);
    serFields(buf, fields...);
    writeString("}", buf);
}

template <typename... T>
std::function<void(WriteBuffer &)> Struct(const T &... fields)
{
    return [fields...](WriteBuffer & buf) { serValue(buf, fields...); };
}

} // namespace JsonSer

namespace TiDB
{

using DB::ReadBufferFromString;
using DB::WriteBuffer;
using DB::WriteBufferFromOwnString;

ColumnInfo::ColumnInfo(const JSON & json) { deserialize(json); }

String ColumnInfo::serialize() const
{
    WriteBufferFromOwnString buf;

    JsonSer::serValue(buf,
        JsonSer::Struct(JsonSer::Field("id", id),
            JsonSer::Field("name", JsonSer::Struct(JsonSer::Field("O", name), JsonSer::Field("L", name))),
            JsonSer::Field("offset", offset),
            JsonSer::Field("origin_default", JsonSer::Nullable(origin_default_value, has_origin_default_value)),
            JsonSer::Field("default", JsonSer::Nullable(default_value, has_default_value)),
            JsonSer::Field("type",
                JsonSer::Struct(
                    // TODO: serialize elems.
                    JsonSer::Field("Tp", tp),
                    JsonSer::Field("Flag", flag),
                    JsonSer::Field("Flen", flen),
                    JsonSer::Field("Decimal", decimal))),
            JsonSer::Field("state", state),
            JsonSer::Field("comment", comment)));

    return buf.str();
}

void ColumnInfo::deserialize(const JSON & json) try
{
    id = json["id"].getInt();
    name = json["name"]["L"].getString();
    offset = static_cast<Int32>(json["offset"].getInt());
    has_origin_default_value = json["origin_default"].isNull();
    origin_default_value = has_origin_default_value ? "" : json["origin_default"].getString();
    has_default_value = json["default"].isNull();
    default_value = has_default_value ? "" : json["default"].getString();
    tp = static_cast<TP>(json["type"]["Tp"].getInt());
    flag = static_cast<UInt32>(json["type"]["Flag"].getInt());
    flen = static_cast<Int32>(json["type"]["Flen"].getInt());
    decimal = static_cast<Int32>(json["type"]["Decimal"].getInt());
    // TODO: deserialize elems.
    state = static_cast<SchemaState>(json["state"].getInt());
    comment = json.getWithDefault<String>("comment", "");
}
catch (const JSONException & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (ColumnInfo): " + e.displayText(), DB::Exception(e));
}

PartitionDefinition::PartitionDefinition(const JSON & json) { deserialize(json); }

String PartitionDefinition::serialize() const
{
    WriteBufferFromOwnString buf;

    JsonSer::serValue(buf,
        JsonSer::Struct(JsonSer::Field("id", id),
            JsonSer::Field("name", JsonSer::Struct(JsonSer::Field("O", name), JsonSer::Field("L", name))),
            JsonSer::Field("comment", comment)));

    return buf.str();
}

void PartitionDefinition::deserialize(const JSON & json) try
{
    id = json["id"].getInt();
    name = json["name"]["L"].getString();
    comment = json.getWithDefault<String>("comment", "");
}
catch (const JSONException & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (PartitionDefinition): " + e.displayText(), DB::Exception(e));
}

PartitionInfo::PartitionInfo(const JSON & json) { deserialize(json); }

String PartitionInfo::serialize() const
{
    WriteBufferFromOwnString buf;

    JsonSer::serValue(buf,
        JsonSer::Struct(JsonSer::Field("type", type),
            JsonSer::Field("expr", expr),
            JsonSer::Field("enable", enable),
            JsonSer::Field("definitions",
                [this]() {
                    std::vector<JsonSer::JsonString> v(definitions.size());
                    std::transform(definitions.begin(), definitions.end(), v.begin(), [](const PartitionDefinition & definition) {
                        return JsonSer::JsonString{definition.serialize()};
                    });
                    return v;
                }()),
            JsonSer::Field("num", num)));

    return buf.str();
}

void PartitionInfo::deserialize(const JSON & json) try
{
    type = static_cast<PartitionType>(json["type"].getInt());
    expr = json["expr"].getString();
    enable = json["enable"].getBool();

    JSON defs_json = json["definitions"];
    definitions.clear();
    for (const auto & def_json : defs_json)
    {
        PartitionDefinition definition(def_json);
        definitions.emplace_back(definition);
    }

    num = static_cast<UInt64>(json["num"].getInt());
}
catch (const JSONException & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (PartitionInfo): " + e.displayText(), DB::Exception(e));
}

TableInfo::TableInfo(const String & table_info_json, bool escaped) { deserialize(table_info_json, escaped); }

String TableInfo::serialize(bool escaped) const
{
    WriteBufferFromOwnString buf;

    JsonSer::serValue(buf,
        JsonSer::Struct(JsonSer::Field("db_info",
                            JsonSer::Struct(JsonSer::Field("id", db_id),
                                JsonSer::Field("db_name", JsonSer::Struct(JsonSer::Field("O", db_name), JsonSer::Field("L", db_name))))),
            JsonSer::Field("table_info",
                JsonSer::Struct(JsonSer::Field("id", id),
                    JsonSer::Field("name", JsonSer::Struct(JsonSer::Field("O", name), JsonSer::Field("L", name))),
                    JsonSer::Field("cols",
                        [this]() {
                            std::vector<JsonSer::JsonString> v(columns.size());
                            std::transform(columns.begin(), columns.end(), v.begin(), [](const ColumnInfo & column) {
                                return JsonSer::JsonString{column.serialize()};
                            });
                            return v;
                        }()),
                    JsonSer::Field("state", state),
                    JsonSer::Field("pk_is_handle", pk_is_handle),
                    JsonSer::Field("comment", comment),
                    JsonSer::Field("update_timestamp", update_timestamp),
                    JsonSer::Field("belonging_table_id", belonging_table_id, !is_partition_table),
                    // TODO: Hack to tell Spark this table is a physical/sub table of a partition.
                    JsonSer::Field("is_partition_sub_table", "true", !(is_partition_table && belonging_table_id != -1)),
                    JsonSer::Field("partition",
                        // lazy serializing partition as it could be null.
                        JsonSer::Nullable(std::function<void(WriteBuffer &)>([this](WriteBuffer & buf) {
                            JsonSer::serValue(buf, JsonSer::JsonString{partition.serialize()});
                        }),
                            !is_partition_table)))),
            JsonSer::Field("schema_version", schema_version)));

    if (!escaped)
    {
        return buf.str();
    }
    else
    {
        WriteBufferFromOwnString escaped_buf;
        writeEscapedString(buf.str(), escaped_buf);
        return escaped_buf.str();
    }
}

void TableInfo::deserialize(const String & json_str, bool escaped) try
{
    if (json_str.empty())
    {
        id = DB::InvalidTableID;
        return;
    }

    String unescaped_json_str;
    if (escaped)
    {
        ReadBufferFromString buf(json_str);
        readEscapedString(unescaped_json_str, buf);
    }
    else
    {
        unescaped_json_str = json_str;
    }

    /// The JSON library does not support whitespace. We delete them. Inefficient.
    // TODO: This may mis-delete innocent spaces/newlines enclosed by quotes, consider using some lexical way.
    ReadBufferFromString in(unescaped_json_str);
    WriteBufferFromOwnString out;
    while (!in.eof())
    {
        char c;
        readChar(c, in);
        if (!isspace(c))
            writeChar(c, out);
    }

    JSON json(out.str());

    JSON db_json = json["db_info"];
    db_id = db_json["id"].getInt();
    db_name = db_json["db_name"]["L"].getString();

    JSON table_json = json["table_info"];
    id = table_json["id"].getInt();
    name = table_json["name"]["L"].getString();
    JSON cols_json = table_json["cols"];
    columns.clear();
    for (const auto & col_json : cols_json)
    {
        ColumnInfo column_info(col_json);
        columns.emplace_back(column_info);
    }
    state = static_cast<SchemaState>(table_json["state"].getInt());
    pk_is_handle = table_json["pk_is_handle"].getBool();
    comment = table_json["comment"].getString();
    update_timestamp = table_json["update_timestamp"].getUInt();
    is_partition_table = !table_json["partition"].isNull();
    if (is_partition_table)
    {
        if (table_json.has("belonging_table_id"))
            belonging_table_id = table_json["belonging_table_id"].getInt();
        partition.deserialize(table_json["partition"]);
    }

    JSON schema_json = json["schema_version"];
    schema_version = schema_json.getInt();
}
catch (const JSONException & e)
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
    for (auto col : columns)
    {
        if (name == col.name)
        {
            return col.id;
        }
    }

    if (name == DB::MutableSupport::tidb_pk_column_name)
        return DB::InvalidColumnID;

    throw DB::Exception(std::string(__PRETTY_FUNCTION__) + ": Unknown column name " + name, DB::ErrorCodes::LOGICAL_ERROR);
}

} // namespace TiDB
