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
#include <Common/CurrentMetrics.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemMetrics.h>


namespace DB
{


StorageSystemMetrics::StorageSystemMetrics(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
        {"metric", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeInt64>()},
    }));
}


BlockInputStreams StorageSystemMetrics::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context &,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        Int64 value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

        res_columns[0]->insert(String(CurrentMetrics::getDescription(CurrentMetrics::Metric(i))));
        res_columns[1]->insert(value);
    }

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}


} // namespace DB
