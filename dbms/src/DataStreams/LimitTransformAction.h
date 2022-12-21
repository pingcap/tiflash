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
#include <common/likely.h>

#include <atomic>
#include <memory>

namespace DB
{
class LocalLimitPos
{
public:
    size_t get()
    {
        return pos;
    }

    size_t addAndGet(size_t rows)
    {
        pos += rows;
        return pos;
    }

private:
    size_t pos = 0;
};

class GlobalLimitPos
{
public:
    size_t get()
    {
        return pos;
    }

    size_t addAndGet(size_t rows)
    {
        size_t pre_pos = pos.fetch_add(rows);
        return pre_pos + rows;
    }

private:
    std::atomic_size_t pos = 0;
};

template <typename LimitPos>
struct LimitTransformAction
{
public:
    LimitTransformAction(
        const Block & header_,
        size_t limit_)
        : header(header_)
        , limit(limit_)
    {
    }

    bool transform(Block & block)
    {
        if (unlikely(!block))
            return true;

        /// pos - how many lines were read, including the last read block
        if (limit_pos.get() >= limit)
        {
            return false;
        }

        auto rows = block.rows();
        auto pos = limit_pos.addAndGet(rows);
        assert(pos >= rows);
        if (pos <= limit)
        {
            // give away the whole block
            return true;
        }
        else
        {
            // pos > limit
            // give away a piece of the block
            assert(rows + limit > pos);
            size_t length = rows + limit - pos;
            for (size_t i = 0; i < block.columns(); ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->cut(0, length);
            return true;
        }
    }

    Block getHeader() const { return header; }
    size_t getLimit() const { return limit; }

private:
    const Block header;
    const size_t limit;
    LimitPos limit_pos;
};

using LocalLimitTransformAction = LimitTransformAction<LocalLimitPos>;
using GlobalLimitTransformAction = LimitTransformAction<GlobalLimitPos>;
using GlobalLimitPtr = std::shared_ptr<GlobalLimitTransformAction>;
} // namespace DB
