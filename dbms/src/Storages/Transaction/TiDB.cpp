#include <Common/Decimal.h>
#include <Common/MyTime.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/TiDB.h>
#include <Poco/Format.h>
#include <Poco/String.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>

namespace TiDB
{
using DB::Decimal128;
using DB::Decimal256;
using DB::Decimal32;
using DB::Decimal64;
using DB::DecimalField;
using DB::Field;

ColumnInfo::ColumnInfo(Poco::JSON::Object::Ptr json) { deserialize(json); }

Field ColumnInfo::defaultValueToField() const
{
    auto & value = origin_default_value;
    if (value.isEmpty())
    {
        return Field();
    }
    switch (tp)
    {
        // TODO: Consider unsigned?
        // Integer Type.
        case TypeTiny:
        case TypeShort:
        case TypeLong:
        case TypeLongLong:
        case TypeInt24:
        case TypeBit:
            return value.convert<Int64>();
        // Floating type.
        case TypeFloat:
        case TypeDouble:
            return value.convert<double>();
        case TypeDate:
        case TypeDatetime:
        case TypeTimestamp:
            return DB::parseMyDateTime(value.convert<String>());
        case TypeVarchar:
        case TypeTinyBlob:
        case TypeMediumBlob:
        case TypeLongBlob:
        case TypeBlob:
        case TypeVarString:
        case TypeString:
            return value.convert<String>();
        case TypeEnum:
            return getEnumIndex(value.convert<String>());
        case TypeNull:
            return Field();
        case TypeDecimal:
        case TypeNewDecimal:
            return getDecimalValue(value.convert<String>());
        case TypeTime:
            return Field();
        case TypeYear:
            return Field();
        case TypeSet:
            return getSetValue(value.convert<String>());
        default:
            throw Exception("Have not processed type: " + std::to_string(tp));
    }
    return Field();
}

    UInt64 ColumnInfo::getSetValue(const String & set_str) const
    {
        // step 1: parse set name
        std::vector<String> splits;
        auto set_str_lowercase = Poco::toLower(set_str);
        boost::split(splits, set_str_lowercase, boost::is_any_of(","));
        std::set<String> marked;
        for (const auto & split : splits)
        {
            marked.insert(split);
        }

        UInt64 value = 0;
        for (size_t i = 0; i < elems.size(); i++)
        {
            String key_lowercase = Poco::toLower(elems.at(i).first);
            auto it = marked.find(key_lowercase);
            if (it != marked.end())
            {
                value |= 1ULL << i;
                marked.erase(it);
            }
        }

        if (marked.empty())
            return value;

        throw DB::Exception(
                std::string(__PRETTY_FUNCTION__) + ": can't parse set type value.");
    }

DB::Field ColumnInfo::getDecimalValue(const String & decimal_text) const
{
    DB::ReadBufferFromString buffer(decimal_text);
    auto precision = flen;
    auto scale = decimal;

    auto type = DB::createDecimal(precision, scale);
    if (DB::checkDecimal<Decimal32>(*type))
    {
        DB::Decimal32 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal32>(result, scale);
    }
    else if (DB::checkDecimal<Decimal64>(*type))
    {
        DB::Decimal64 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal64>(result, scale);
    }
    else if (DB::checkDecimal<Decimal128>(*type))
    {
        DB::Decimal128 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal128>(result, scale);
    }
    else
    {
        DB::Decimal256 result;
        DB::readDecimalText(result, buffer, precision, scale);
        return DecimalField<Decimal256>(result, scale);
    }
}

// FIXME it still has bug: https://github.com/pingcap/tidb/issues/11435
Int64 ColumnInfo::getEnumIndex(const String & enum_id_or_text) const
{
    for (const auto & elem : elems)
    {
        if (elem.first == enum_id_or_text)
        {
            return elem.second;
        }
    }
    int num = std::stoi(enum_id_or_text);
    return num;
}

Poco::JSON::Object::Ptr ColumnInfo::getJSONObject() const try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();

    json->set("id", id);
    Poco::JSON::Object::Ptr name_json = new Poco::JSON::Object();
    name_json->set("O", name);
    name_json->set("L", name);
    json->set("name", name_json);
    json->set("offset", offset);
    json->set("origin_default", origin_default_value);
    json->set("default", default_value);
    Poco::JSON::Object::Ptr tp_json = new Poco::JSON::Object();
    tp_json->set("Tp", static_cast<Int32>(tp));
    tp_json->set("Flag", flag);
    tp_json->set("Flen", flen);
    tp_json->set("Decimal", decimal);
    if (!elems.empty())
    {
        Poco::JSON::Array::Ptr elem_arr = new Poco::JSON::Array();
        for (auto & elem : elems)
            elem_arr->add(elem.first);
        tp_json->set("Elems", elem_arr);
    }
    else
    {
        tp_json->set("Elems", Poco::Dynamic::Var());
    }
    json->set("type", tp_json);
    json->set("state", static_cast<Int32>(state));
    json->set("comment", comment);

    std::stringstream str;
    json->stringify(str);

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (ColumnInfo): " + e.displayText(), DB::Exception(e));
}

void ColumnInfo::deserialize(Poco::JSON::Object::Ptr json) try
{
    id = json->getValue<Int64>("id");
    name = json->getObject("name")->getValue<String>("L");
    offset = json->getValue<Int32>("offset");
    if (!json->isNull("origin_default"))
        origin_default_value = json->get("origin_default");
    if (!json->isNull("default"))
        default_value = json->get("default");
    auto type_json = json->getObject("type");
    tp = static_cast<TP>(type_json->getValue<Int32>("Tp"));
    flag = type_json->getValue<UInt32>("Flag");
    flen = type_json->getValue<Int64>("Flen");
    decimal = type_json->getValue<Int64>("Decimal");
    if (!type_json->isNull("Elems"))
    {
        auto elems_arr = type_json->getArray("Elems");
        size_t elems_size = elems_arr->size();
        for (size_t i = 1; i <= elems_size; i++)
        {
            elems.push_back(std::make_pair(elems_arr->getElement<String>(i - 1), Int16(i)));
        }
    }
    state = static_cast<SchemaState>(json->getValue<Int32>("state"));
    comment = json->getValue<String>("comment");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (ColumnInfo): " + e.displayText(), DB::Exception(e));
}

PartitionDefinition::PartitionDefinition(Poco::JSON::Object::Ptr json) { deserialize(json); }

Poco::JSON::Object::Ptr PartitionDefinition::getJSONObject() const try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("id", id);
    Poco::JSON::Object::Ptr name_json = new Poco::JSON::Object();
    name_json->set("O", name);
    name_json->set("L", name);
    json->set("name", name_json);
    json->set("comment", comment);

    std::stringstream str;
    json->stringify(str);

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (PartitionDef): " + e.displayText(), DB::Exception(e));
}

void PartitionDefinition::deserialize(Poco::JSON::Object::Ptr json) try
{
    id = json->getValue<Int64>("id");
    name = json->getObject("name")->getValue<String>("L");
    if (json->has("comment"))
        comment = json->getValue<String>("comment");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (PartitionDefinition): " + e.displayText(), DB::Exception(e));
}

PartitionInfo::PartitionInfo(Poco::JSON::Object::Ptr json) { deserialize(json); }

Poco::JSON::Object::Ptr PartitionInfo::getJSONObject() const try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();

    json->set("type", static_cast<Int32>(type));
    json->set("expr", expr);
    json->set("enable", enable);
    json->set("num", num);

    Poco::JSON::Array::Ptr def_arr = new Poco::JSON::Array();

    for (auto & part_def : definitions)
    {
        def_arr->add(part_def.getJSONObject());
    }

    json->set("definitions", def_arr);

    std::stringstream str;
    json->stringify(str);

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (PartitionInfo): " + e.displayText(), DB::Exception(e));
}

void PartitionInfo::deserialize(Poco::JSON::Object::Ptr json) try
{
    type = static_cast<PartitionType>(json->getValue<Int32>("type"));
    expr = json->getValue<String>("expr");
    enable = json->getValue<bool>("enable");

    auto defs_json = json->getArray("definitions");
    definitions.clear();
    for (size_t i = 0; i < defs_json->size(); i++)
    {
        PartitionDefinition definition(defs_json->getObject(i));
        definitions.emplace_back(definition);
    }

    num = json->getValue<UInt64>("num");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (PartitionInfo): " + e.displayText(), DB::Exception(e));
}

TableInfo::TableInfo(const String & table_info_json) { deserialize(table_info_json); }

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
    for (auto & col_info : columns)
    {
        auto col_obj = col_info.getJSONObject();
        cols_arr->add(col_obj);
    }

    json->set("cols", cols_arr);
    json->set("state", static_cast<Int32>(state));
    json->set("pk_is_handle", pk_is_handle);
    json->set("comment", comment);
    json->set("update_timestamp", update_timestamp);
    if (is_partition_table)
    {
        json->set("belonging_table_id", belonging_table_id);
        json->set("partition", partition.getJSONObject());
        if (belonging_table_id != -1)
        {
            json->set("is_partition_sub_table", true);
        }
    }
    else
    {
        json->set("partition", Poco::Dynamic::Var());
    }

    json->set("schema_version", schema_version);

    json->stringify(buf);

    return buf.str();
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (TableInfo): " + e.displayText(), DB::Exception(e));
}

void DBInfo::deserialize(const String & json_str) try
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json_str);
    auto obj = result.extract<Poco::JSON::Object::Ptr>();
    id = obj->getValue<Int64>("id");
    name = obj->get("db_name").extract<Poco::JSON::Object::Ptr>()->get("L").convert<String>();
    charset = obj->get("charset").convert<String>();
    collate = obj->get("collate").convert<String>();
    state = static_cast<SchemaState>(obj->getValue<Int32>("state"));
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (DBInfo): " + e.displayText() + ", json: " + json_str,
        DB::Exception(e));
}

void TableInfo::deserialize(const String & json_str) try
{
    if (json_str.empty())
    {
        id = DB::InvalidTableID;
        return;
    }

    Poco::JSON::Parser parser;

    Poco::Dynamic::Var result = parser.parse(json_str);

    auto obj = result.extract<Poco::JSON::Object::Ptr>();
    id = obj->getValue<Int64>("id");
    name = obj->getObject("name")->getValue<String>("L");

    auto cols_arr = obj->getArray("cols");
    columns.clear();
    for (size_t i = 0; i < cols_arr->size(); i++)
    {
        auto col_json = cols_arr->getObject(i);
        ColumnInfo column_info(col_json);
        columns.emplace_back(column_info);
    }

    state = static_cast<SchemaState>(obj->getValue<Int32>("state"));
    pk_is_handle = obj->getValue<bool>("pk_is_handle");
    comment = obj->getValue<String>("comment");
    update_timestamp = obj->getValue<Timestamp>("update_timestamp");
    auto partition_obj = obj->getObject("partition");
    is_partition_table = !partition_obj.isNull();
    if (is_partition_table)
    {
        if (obj->has("belonging_table_id"))
            belonging_table_id = obj->getValue<TableID>("belonging_table_id");
        partition.deserialize(partition_obj);
    }
    if (obj->has("schema_version"))
    {
        schema_version = obj->getValue<Int64>("schema_version");
    }
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

ColumnID TableInfo::getColumnID(const String & col_name) const
{
    for (auto & col : columns)
    {
        if (col_name == col.name)
        {
            return col.id;
        }
    }

    if (col_name == DB::MutableSupport::tidb_pk_column_name)
        return DB::InvalidColumnID;

    throw DB::Exception(std::string(__PRETTY_FUNCTION__) + ": Unknown column name " + name, DB::ErrorCodes::LOGICAL_ERROR);
}

String TableInfo::getColumnName(const ColumnID col_id) const
{
    for (auto & col : columns)
    {
        if (col_id == col.id)
        {
            return col.name;
        }
    }

    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Invalidate column id " + std::to_string(id) + " for table " + db_name + "." + name,
        DB::ErrorCodes::LOGICAL_ERROR);
}

std::optional<std::reference_wrapper<const ColumnInfo>> TableInfo::getPKHandleColumn() const
{
    if (!pk_is_handle)
        return std::nullopt;

    for (auto & col : columns)
    {
        if (col.hasPriKeyFlag())
            return std::optional<std::reference_wrapper<const ColumnInfo>>(col);
    }

    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Cannot get handle column for table " + db_name + "." + name, DB::ErrorCodes::LOGICAL_ERROR);
}

TableInfo TableInfo::producePartitionTableInfo(TableID table_or_partition_id) const
{
    // Some sanity checks for partition table.
    if (unlikely(!(is_partition_table && partition.enable)))
        throw Exception("Table ID " + std::to_string(id) + " seeing partition ID " + std::to_string(table_or_partition_id)
                + " but it's not a partition table",
            DB::ErrorCodes::LOGICAL_ERROR);

    if (unlikely(std::find_if(partition.definitions.begin(), partition.definitions.end(), [table_or_partition_id](const auto & d) {
            return d.id == table_or_partition_id;
        }) == partition.definitions.end()))
        throw Exception("Couldn't find partition with ID " + std::to_string(table_or_partition_id) + " in table ID " + std::to_string(id),
            DB::ErrorCodes::LOGICAL_ERROR);

    // This is a TiDB partition table, adjust the table ID by making it to physical table ID (partition ID).
    TableInfo new_table = *this;
    new_table.belonging_table_id = id;
    new_table.id = table_or_partition_id;

    // Mangle the table name by appending partition name.
    new_table.name = getPartitionTableName(table_or_partition_id);

    return new_table;
}

String TableInfo::getPartitionTableName(TableID part_id) const { return name + "_" + std::to_string(part_id); }

} // namespace TiDB
