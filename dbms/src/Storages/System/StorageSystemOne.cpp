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

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemOne.h>


namespace DB
{


StorageSystemOne::StorageSystemOne(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({{"dummy", std::make_shared<DataTypeUInt8>()}}));
}


BlockInputStreams StorageSystemOne::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context &,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    return BlockInputStreams(
        1,
        std::make_shared<OneBlockInputStream>(Block{ColumnWithTypeAndName(
            DataTypeUInt8().createColumnConst(1, UInt64(0))->convertToFullColumnIfConst(),
            std::make_shared<DataTypeUInt8>(),
            "dummy")}));
}


} // namespace DB
