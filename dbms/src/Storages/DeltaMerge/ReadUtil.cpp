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

#include <Storages/DeltaMerge/ReadUtil.h>


namespace DB::DM
{

std::pair<Block, bool> readBlock(SkippableBlockInputStreamPtr & stable, SkippableBlockInputStreamPtr & delta)
{
    if (stable == nullptr && delta == nullptr)
    {
        return {{}, false};
    }

    if (stable == nullptr)
    {
        return {delta->read(), true};
    }

    auto block = stable->read();
    if (block)
    {
        return {block, false};
    }
    else
    {
        stable = nullptr;
        if (delta != nullptr)
        {
            block = delta->read();
        }
        return {block, true};
    }
}

size_t skipBlock(SkippableBlockInputStreamPtr & stable, SkippableBlockInputStreamPtr & delta)
{
    if (stable == nullptr && delta == nullptr)
    {
        return 0;
    }

    if (stable == nullptr)
    {
        return delta->skipNextBlock();
    }

    if (size_t skipped_rows = stable->skipNextBlock(); skipped_rows > 0)
    {
        return skipped_rows;
    }
    else
    {
        stable = nullptr;
        if (delta != nullptr)
        {
            return delta->skipNextBlock();
        }
        return 0;
    }
}

std::pair<Block, bool> readBlockWithFilter(
    SkippableBlockInputStreamPtr & stable,
    SkippableBlockInputStreamPtr & delta,
    const IColumn::Filter & filter,
    FilterPtr & res_filter,
    bool return_filter)
{
    if (filter.empty())
    {
        return {{}, false};
    }

    if (stable == nullptr && delta == nullptr)
    {
        return {{}, false};
    }

    if (stable == nullptr)
    {
        return {delta->readWithFilter(filter, res_filter, return_filter), true};
    }

    auto block = stable->readWithFilter(filter, res_filter, return_filter);
    if (block)
    {
        return {block, false};
    }
    else
    {
        stable = nullptr;
        if (delta != nullptr)
        {
            block = delta->readWithFilter(filter, res_filter, return_filter);
        }
        return {block, true};
    }
}

std::pair<Block, bool> readBlock(
    SkippableBlockInputStreamPtr & stable,
    SkippableBlockInputStreamPtr & delta,
    FilterPtr & res_filter,
    bool return_filter)
{
    if (stable == nullptr && delta == nullptr)
    {
        return {{}, false};
    }

    if (stable == nullptr)
    {
        return {delta->read(res_filter, return_filter), true};
    }

    auto block = stable->read(res_filter, return_filter);
    if (block)
    {
        return {block, false};
    }
    else
    {
        stable = nullptr;
        if (delta != nullptr)
        {
            block = delta->read(res_filter, return_filter);
        }
        return {block, true};
    }
}
} // namespace DB::DM
