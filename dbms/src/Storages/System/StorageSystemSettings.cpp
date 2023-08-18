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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemSettings.h>


namespace DB
{


StorageSystemSettings::StorageSystemSettings(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
        {"changed", std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
    }));
}


BlockInputStreams StorageSystemSettings::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    const Settings & settings = context.getSettingsRef();

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

#define ADD_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION)      \
    res_columns[0]->insert(String(#NAME));                 \
    res_columns[1]->insert(settings.NAME.toString());      \
    res_columns[2]->insert(UInt64(settings.NAME.changed)); \
    res_columns[3]->insert(String(DESCRIPTION));
    APPLY_FOR_SETTINGS(ADD_SETTING)
#undef ADD_SETTING

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB
