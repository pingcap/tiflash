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
#include <Common/Decimal.h>
#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <Common/types ̰.h>
#include <DataTypes/DataTypeDecimal.h>
#include <Dictionaries/DictionaryStructure.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/Base64Decoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/StringTokenizer.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Collator.h>
#include <Storages/Transaction/TiDB.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <common/logger_useful.h>

#include <cmath>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
extern const UInt8 TYPE_CODE_LITERAL;
extern const UInt8 LITERAL_NIL;

Field GenDefaultField(const TiDB::ColumnInfo & col_info)
{
    switch (col_info.getCodecFlag())
    {
    case TiDB::CodecFlagNil:
        return Field();
    case TiDB::CodecFlagBytes:
        return Field(String());
    case TiDB::CodecFlagDecimal:
    {
        auto type = createDecimal(col_info.flen, col_info.decimal);
        if (checkDecimal<Decimal32>(*type))
            return Field(DecimalField<Decimal32>(Decimal32(), col_info.decimal));
        else if (checkDecimal<Decimal64>(*type))
            return Field(DecimalField<Decimal64>(Decimal64(), col_info.decimal));
        else if (checkDecimal<Decimal128>(*type))
            return Field(DecimalField<Decimal128>(Decimal128(), col_info.decimal));
        else
            return Field(DecimalField<Decimal256>(Decimal256(), col_info.decimal));
    }
    break;
    case TiDB::CodecFlagCompactBytes:
        return Field(String());
    case TiDB::CodecFlagFloat:
        return Field(static_cast<Float64>(0));
    case TiDB::CodecFlagUInt:
        return Field(static_cast<UInt64>(0));
    case TiDB::CodecFlagInt:
        return Field(static_cast<Int64>(0));
    case TiDB::CodecFlagVarInt:
        return Field(static_cast<Int64>(0));
    case TiDB::CodecFlagVarUInt:
        return Field(static_cast<UInt64>(0));
    case TiDB::CodecFlagJson:
        return TiDB::genJsonNull();
    case TiDB::CodecFlagDuration:
        return Field(static_cast<Int64>(0));
    default:
        throw Exception("Not implemented codec flag: " + std::to_string(col_info.getCodecFlag()), ErrorCodes::LOGICAL_ERROR);
    }
}
} // namespace DB

namespace TiDB
{
using DB::Decimal128;
using DB::Decimal256;
using DB::Decimal32;
using DB::Decimal64;
using DB::DecimalField;
using DB::Exception;
using DB::Field;
using DB::SchemaNameMapper;

///////////////////////////
////// PartitionInfo //////
///////////////////////////

PartitionDefinition::PartitionDefinition(Poco::JSON::Object::Ptr json)
{
    deserialize(json);
}

Poco::JSON::Object::Ptr PartitionDefinition::getJSONObject() const
try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    json->set("id", id);
    Poco::JSON::Object::Ptr name_json = new Poco::JSON::Object();
    name_json->set("O", name);
    name_json->set("L", name);
    json->set("name", name_json);
    json->set("comment", comment);

#ifndef NDEBUG
    // Check stringify in Debug mode
    std::stringstream str;
    json->stringify(str);
#endif

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (PartitionDef): " + e.displayText(),
        DB::Exception(e));
}

void PartitionDefinition::deserialize(Poco::JSON::Object::Ptr json)
try
{
    id = json->getValue<TableID>("id");
    name = json->getObject("name")->getValue<String>("L");
    if (json->has("comment"))
        comment = json->getValue<String>("comment");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (PartitionDefinition): " + e.displayText(),
        DB::Exception(e));
}

PartitionInfo::PartitionInfo(Poco::JSON::Object::Ptr json)
{
    deserialize(json);
}

Poco::JSON::Object::Ptr PartitionInfo::getJSONObject() const
try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();

    json->set("type", static_cast<Int32>(type));
    json->set("expr", expr);
    json->set("enable", enable);
    json->set("num", num);

    Poco::JSON::Array::Ptr def_arr = new Poco::JSON::Array();

    for (const auto & part_def : definitions)
    {
        def_arr->add(part_def.getJSONObject());
    }

    json->set("definitions", def_arr);

#ifndef NDEBUG
    // Check stringify in Debug mode
    std::stringstream str;
    json->stringify(str);
#endif

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (PartitionInfo): " + e.displayText(),
        DB::Exception(e));
}

void PartitionInfo::deserialize(Poco::JSON::Object::Ptr json)
try
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
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (PartitionInfo): " + e.displayText(),
        DB::Exception(e));
}

///////////////////////
/// IndexColumnInfo ///
///////////////////////

IndexColumnInfo::IndexColumnInfo(Poco::JSON::Object::Ptr json)
    : length(0)
    , offset(0)
{
    deserialize(json);
}

Poco::JSON::Object::Ptr IndexColumnInfo::getJSONObject() const
try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();

    Poco::JSON::Object::Ptr name_json = new Poco::JSON::Object();
    name_json->set("O", name);
    name_json->set("L", name);
    json->set("name", name_json);
    json->set("offset", offset);
    json->set("length", length);

#ifndef NDEBUG
    std::stringstream str;
    json->stringify(str);
#endif

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (IndexColumnInfo): " + e.displayText(),
        DB::Exception(e));
}

void IndexColumnInfo::deserialize(Poco::JSON::Object::Ptr json)
try
{
    name = json->getObject("name")->getValue<String>("L");
    offset = json->getValue<Int32>("offset");
    length = json->getValue<Int32>("length");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Parse TiDB schema JSON failed (IndexColumnInfo): " + e.displayText(),
        DB::Exception(e));
}

} // namespace TiDB
