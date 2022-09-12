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

#pragma once

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>

#include <atomic>
#include <memory>

namespace DB
{
class LimitBreaker
{
public:
    explicit LimitBreaker(
        size_t limit_)
        : limit(limit_)
    {
        if (limit == 0)
            is_stop = true;
    }

    bool insert(Block && block)
    {
        if (is_stop)
            return false;

        std::lock_guard<std::mutex> lock(mu);
        if (is_stop)
            return false;

        size_t rows = block.rows();
        cur_rows += rows;
        if (cur_rows < limit)
        {
            blocks.emplace_back(std::move(block));
            return true;
        }
        else if (cur_rows == limit)
        {
            is_stop = true;
            blocks.emplace_back(std::move(block));
            return false;
        }
        else // cur_rows > limit
        {
            is_stop = true;
            assert((rows + limit) > cur_rows);
            size_t length = rows + limit - cur_rows;
            for (size_t i = 0; i < block.columns(); ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->cut(0, length);
            blocks.emplace_back(std::move(block));
            return false;
        }
    }

    Block read()
    {
        std::lock_guard<std::mutex> lock(mu);
        if (blocks.empty())
            return {};

        Block block = std::move(blocks.back());
        blocks.pop_back();
        return block;
    }

    void initHeader(const Block & header_)
    {
        header = header_;
    }

    Block getHeader()
    {
        return header;
    }

private:
    Block header;
    size_t limit;

    std::atomic_bool is_stop = false;
    std::mutex mu;
    size_t cur_rows = 0;
    Blocks blocks;
};
using LimitBreakerPtr = std::shared_ptr<LimitBreaker>;
} // namespace DB
