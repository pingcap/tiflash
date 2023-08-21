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

#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataStreams/GeneratedColumnPlaceHolderTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Flash/Coprocessor/TiDBTableScan.h>

namespace DB
{
class GeneratedColumnPlaceholderBlockInputStream : public IProfilingBlockInputStream
{
public:
    GeneratedColumnPlaceholderBlockInputStream(
        const BlockInputStreamPtr & input,
        const std::vector<std::tuple<UInt64, String, DataTypePtr>> & generated_column_infos_,
        const String & req_id_)
        : action(input->getHeader(), generated_column_infos_)
        , log(Logger::get(req_id_))
    {
        children.push_back(input);
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return action.getHeader(); }

    static String getColumnName(UInt64 col_index) { return "generated_column_" + std::to_string(col_index); }

protected:
    void readPrefix() override { action.checkColumn(); }

    Block readImpl() override
    {
        Block block = children.back()->read();
        action.transform(block);
        return block;
    }

private:
    static constexpr auto NAME = "GeneratedColumnPlaceholder";
    GeneratedColumnPlaceHolderTransformAction action;
    const LoggerPtr log;
};

} // namespace DB
