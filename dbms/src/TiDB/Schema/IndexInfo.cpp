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
#include <IO/ReadBufferFromString.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/SchemaState.h>
#include <TiDB/Schema/IndexInfo.h>

namespace TiDB
{ 

IndexInfo::IndexInfo(Poco::JSON::Object::Ptr json)
    : id(0)
    , state(TiDB::SchemaState::StateNone)
    , index_type(0)
    , is_unique(true)
    , is_primary(true)
    , is_invisible(true)
    , is_global(true)
{
    deserialize(json);
}
Poco::JSON::Object::Ptr IndexInfo::getJSONObject() const
try
{
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();

    json->set("id", id);

    Poco::JSON::Object::Ptr idx_name_json = new Poco::JSON::Object();
    idx_name_json->set("O", idx_name);
    idx_name_json->set("L", idx_name);
    json->set("idx_name", idx_name_json);

    Poco::JSON::Object::Ptr tbl_name_json = new Poco::JSON::Object();
    tbl_name_json->set("O", tbl_name);
    tbl_name_json->set("L", tbl_name);
    json->set("tbl_name", tbl_name_json);

    Poco::JSON::Array::Ptr cols_array = new Poco::JSON::Array();
    for (const auto & col : idx_cols)
    {
        auto col_obj = col.getJSONObject();
        cols_array->add(col_obj);
    }
    json->set("idx_cols", cols_array);
    json->set("state", static_cast<Int32>(state));
    json->set("index_type", index_type);
    json->set("is_unique", is_unique);
    json->set("is_primary", is_primary);
    json->set("is_invisible", is_invisible);
    json->set("is_global", is_global);

#ifndef NDEBUG
    std::stringstream str;
    json->stringify(str);
#endif

    return json;
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Serialize TiDB schema JSON failed (IndexInfo): " + e.displayText(),
        DB::Exception(e));
}

void IndexInfo::deserialize(Poco::JSON::Object::Ptr json)
try
{
    id = json->getValue<Int64>("id");
    idx_name = json->getObject("idx_name")->getValue<String>("L");
    tbl_name = json->getObject("tbl_name")->getValue<String>("L");

    auto cols_array = json->getArray("idx_cols");
    idx_cols.clear();
    if (!cols_array.isNull())
    {
        for (size_t i = 0; i < cols_array->size(); i++)
        {
            auto col_json = cols_array->getObject(i);
            IndexColumnInfo column_info(col_json);
            idx_cols.emplace_back(column_info);
        }
    }

    state = static_cast<SchemaState>(json->getValue<Int32>("state"));
    index_type = json->getValue<Int32>("index_type");
    is_unique = json->getValue<bool>("is_unique");
    is_primary = json->getValue<bool>("is_primary");
    if (json->has("is_invisible"))
        is_invisible = json->getValue<bool>("is_invisible");
    if (json->has("is_global"))
        is_global = json->getValue<bool>("is_global");
}
catch (const Poco::Exception & e)
{
    throw DB::Exception(
        std::string(__PRETTY_FUNCTION__) + ": Deserialize TiDB schema JSON failed (IndexInfo): " + e.displayText(),
        DB::Exception(e));
}

String TiFlashModeToString(TiFlashMode tiflash_mode)
{
    switch (tiflash_mode)
    {
    case TiFlashMode::Normal:
        return "";
    case TiFlashMode::Fast:
        return "fast";
    default:
        LOG_FMT_WARNING(&Poco::Logger::get("TiDB"), "TiFlashModeToString with invalid tiflash mode {}", tiflash_mode);
        return "";
    }
}

TiFlashMode parseTiFlashMode(String mode_str)
{
    if (mode_str.empty())
    {
        return TiFlashMode::Normal;
    }
    else if (mode_str == "fast")
    {
        return TiFlashMode::Fast;
    }
    else
    {
        throw DB::Exception(
            std::string(__PRETTY_FUNCTION__)
            + " ParseTiFlashMode Failed. mode " + mode_str + " is unvalid, please set mode as fast/normal");
    }
}

tipb::FieldType columnInfoToFieldType(const ColumnInfo & ci)
{
    tipb::FieldType ret;
    ret.set_tp(ci.tp);
    ret.set_flag(ci.flag);
    ret.set_flen(ci.flen);
    ret.set_decimal(ci.decimal);
    for (const auto & elem : ci.elems)
    {
        ret.add_elems(elem.first);
    }
    return ret;
}

ColumnInfo fieldTypeToColumnInfo(const tipb::FieldType & field_type)
{
    TiDB::ColumnInfo ret;
    ret.tp = static_cast<TiDB::TP>(field_type.tp());
    ret.flag = field_type.flag();
    ret.flen = field_type.flen();
    ret.decimal = field_type.decimal();
    for (int i = 0; i < field_type.elems_size(); i++)
    {
        ret.elems.emplace_back(field_type.elems(i), i + 1);
    }
    return ret;
}

ColumnInfo toTiDBColumnInfo(const tipb::ColumnInfo & tipb_column_info)
{
    ColumnInfo tidb_column_info;
    tidb_column_info.tp = static_cast<TiDB::TP>(tipb_column_info.tp());
    tidb_column_info.id = tipb_column_info.column_id();
    tidb_column_info.flag = tipb_column_info.flag();
    tidb_column_info.flen = tipb_column_info.columnlen();
    tidb_column_info.decimal = tipb_column_info.decimal();
    for (int i = 0; i < tipb_column_info.elems_size(); ++i)
        tidb_column_info.elems.emplace_back(tipb_column_info.elems(i), i + 1);
    return tidb_column_info;
}

} // namespace TiDB
