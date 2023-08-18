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

#include <Columns/ColumnString.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

class StringStreamBlockInputStream : public IProfilingBlockInputStream
{
private:
    Block readImpl() override
    {
        Block block;
        if (data.empty())
            return block;

        auto col = ColumnString::create();
        for (const auto & str : data)
            col->insert(Field(str));
        data.clear();
        block.insert({std::move(col), std::make_shared<DataTypeString>(), col_name});
        return block;
    }

    Block getHeader() const override
    {
        Block block;
        auto col = ColumnString::create();
        block.insert({std::move(col), std::make_shared<DataTypeString>(), col_name});
        return block;
    }

public:
    StringStreamBlockInputStream(const std::string col_name_)
        : col_name(col_name_)
    {}

    void append(const std::string & s) { data.emplace_back(s); }

    String getName() const override { return "StringStreamInput"; }

private:
    std::vector<std::string> data;
    const std::string col_name;
};

} // namespace DB
