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

#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

#include <unordered_set>


namespace DB
{
namespace DM
{
class DMColumnFilterBlockInputStream : public IBlockInputStream
{
public:
    DMColumnFilterBlockInputStream(const BlockInputStreamPtr & input, const ColumnDefines & columns_to_read_)
        : columns_to_read(columns_to_read_)
        , header(toEmptyBlock(columns_to_read))
    {
        children.emplace_back(input);
    }

    String getName() const override { return "DMColumnFilter"; }

    Block getHeader() const override { return header; }

    Block read() override
    {
        Block block = children.back()->read();
        if (!block)
            return {};
        Block res;
        for (auto & cd : columns_to_read)
        {
            res.insert(block.getByName(cd.name));
        }
        return res;
    }

private:
    ColumnDefines columns_to_read;
    Block header;
};

class DMHandleConvertBlockInputStream : public IBlockInputStream
{
public:
    using ColumnNames = std::vector<std::string>;

    DMHandleConvertBlockInputStream(const BlockInputStreamPtr & input,
                                    const String & handle_name_,
                                    const DataTypePtr & handle_original_type_,
                                    const Context & context_)
        : handle_name(handle_name_)
        , handle_original_type(handle_original_type_)
        , context(context_)
    {
        children.emplace_back(input);
    }

    String getName() const override { return "DMHandleConvert"; }

    Block getHeader() const override { return children.back()->getHeader(); }

    Block read() override
    {
        Block block = children.back()->read();
        if (!block)
            return {};
        if (handle_original_type && block.has(handle_name))
        {
            auto pos = block.getPositionByName(handle_name);
            convertColumn(block, pos, handle_original_type, context);
            block.getByPosition(pos).type = handle_original_type;
        }
        return block;
    }

private:
    Block header;
    String handle_name;
    DataTypePtr handle_original_type;
    const Context & context;
};

} // namespace DM
} // namespace DB
