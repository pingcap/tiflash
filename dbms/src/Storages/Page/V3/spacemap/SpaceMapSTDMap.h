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
#include <Common/Exception.h>
#include <Storages/Page/V3/MapUtils.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <fmt/format.h>

#include <ext/shared_ptr_helper.h>
#include <map>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace PS::V3
{
class STDMapSpaceMap
    : public SpaceMap
    , public ext::SharedPtrHelper<STDMapSpaceMap>
{
public:
    ~STDMapSpaceMap() override = default;

    bool check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker, size_t size) override
    {
        size_t idx = 0;
        for (const auto & [offset, length] : free_map)
        {
            if (!checker(idx, offset, offset + length))
                return false;
            idx++;
        }

        return idx == size;
    }

protected:
    STDMapSpaceMap(UInt64 start, UInt64 end)
        : SpaceMap(start, end, SMAP64_STD_MAP)
    {
        free_map.insert({start, end});
    }

    String toDebugString() override
    {
        UInt64 count = 0;

        FmtBuffer fmt_buffer;
        fmt_buffer.append("    STD-Map entries status: \n");

        // Need use `count`,so can't use `joinStr` here.
        for (auto it = free_map.begin(); it != free_map.end(); it++)
        {
            fmt_buffer.fmtAppend("      Space: {} start: {} size : {}\n", count, it->first, it->second);
            count++;
        }

        return fmt_buffer.toString();
    }

    std::pair<UInt64, UInt64> getSizes() const override
    {
        if (free_map.empty())
        {
            auto range = end - start;
            return std::make_pair(range, range);
        }

        const auto & last_free_block = free_map.rbegin();

        if (last_free_block->first + last_free_block->second != end)
        {
            UInt64 total_size = end - start;
            UInt64 valid_size = total_size;
            for (const auto & free_block : free_map)
            {
                valid_size -= free_block.second;
            }

            return std::make_pair(total_size, valid_size);
        }
        else
        {
            UInt64 total_size = last_free_block->first - start;
            UInt64 last_free_block_size = last_free_block->second;

            UInt64 valid_size = 0;
            for (const auto & free_block : free_map)
            {
                valid_size += free_block.second;
            }
            valid_size = total_size - (valid_size - last_free_block_size);

            return std::make_pair(total_size, valid_size);
        }
    }

    UInt64 getUsedBoundary() override
    {
        if (free_map.empty())
        {
            return end;
        }

        const auto & last_node_it = free_map.rbegin();

        // If the `offset+size` of the last free node is not equal to `end`, it means the range `[last_node.offset, end)` is marked as used,
        // then we should return `end` as the used boundary.
        //
        // eg.
        //  1. The spacemap manage a space of `[0, 100]`
        //  2. A span {offset=90, size=10} is marked as used, then the free range in SpaceMap is `[0, 90)`
        //  3. The return value should be 100
        if (last_node_it->first + last_node_it->second != end)
        {
            return end;
        }

        // Else we should return the offset of last free node
        return last_node_it->first;
    }

    bool isMarkUnused(UInt64 offset, size_t length) override
    {
        auto it = MapUtils::findLessEQ(free_map, offset); // first free block <= `offset`
        if (it == free_map.end())
        {
            // No free blocks <= `offset`
            return false;
        }

        return (it->first <= offset && (it->first + it->second >= offset + length));
    }

    bool markUsedImpl(UInt64 offset, size_t length) override
    {
        auto it = MapUtils::findLessEQ(free_map, offset); // first free block <= `offset`
        if (it == free_map.end())
        {
            return false;
        }

        // already been marked used
        if (it->first + it->second < offset)
        {
            return false;
        }

        if (length > it->second || it->first + it->second < offset + length)
        {
            LOG_FMT_WARNING(log, "Marked space used failed. [offset={}, size={}] is bigger than space [offset={},size={}]", offset, length, it->first, it->second);
            return false;
        }

        // match
        if (it->first == offset)
        {
            if (length == it->second)
            {
                free_map.erase(it);
            }
            else
            {
                // Shrink the free block from left
                auto shrink_offset = it->first + length;
                auto shrink_size = it->second - length;
                free_map.erase(it);
                free_map[shrink_offset] = shrink_size;
            }
        }
        else if (it->first + it->second == offset + length)
        {
            // Shrink the free block from right
            assert(it->second != length); // should not run into here
            free_map[it->first] = it->second - length;
        }
        else
        {
            // In the mid, and not match the left or right.
            // Split to two space
            free_map.insert({offset + length, it->first + it->second - offset - length});
            free_map[it->first] = offset - it->first;
        }

        return true;
    }

    std::tuple<UInt64, UInt64, bool> searchInsertOffset(size_t size) override
    {
        UInt64 offset = UINT64_MAX, last_offset = UINT64_MAX;
        UInt64 max_cap = 0;
        // The biggest free block capacity and its start offset
        UInt64 scan_biggest_cap = 0;
        UInt64 scan_biggest_offset = 0;

        if (free_map.empty())
        {
            LOG_FMT_ERROR(log, "Current space map is full");
            hint_biggest_cap = 0;
            return std::make_tuple(offset, hint_biggest_cap, false);
        }

        auto r_it = free_map.rbegin();
        last_offset = (r_it->first + r_it->second == end) ? r_it->first : UINT64_MAX;

        auto it = free_map.begin();
        for (; it != free_map.end(); it++)
        {
            if (it->second >= size)
            {
                break;
            }
            // Keep track of the biggest free block we scanned before `it`
            if (it->second > scan_biggest_cap)
            {
                scan_biggest_cap = it->second;
                scan_biggest_offset = it->first;
            }
        }

        // No enough space for insert
        if (it == free_map.end())
        {
            LOG_FMT_ERROR(log, "Not sure why can't found any place to insert."
                               "[size={}] [old biggest_offset={}] [old biggest_cap={}] [new biggest_offset={}] [new biggest_cap={}]", //
                          size,
                          hint_biggest_offset,
                          hint_biggest_cap,
                          scan_biggest_offset,
                          scan_biggest_cap);
            hint_biggest_offset = scan_biggest_offset;
            hint_biggest_cap = scan_biggest_cap;

            return std::make_tuple(offset, hint_biggest_cap, false);
        }

        // Update return start
        offset = it->first;

        if (it->second == size)
        {
            // It is not champion, just return
            if (it->first != hint_biggest_offset)
            {
                free_map.erase(it);
                max_cap = hint_biggest_cap;
                return std::make_tuple(offset, max_cap, last_offset == offset);
            }

            // It is champion, need to update `scan_biggest_cap`, `scan_biggest_offset`
            // and scan other free blocks to update `biggest_offset` and `biggest_cap`
            it = free_map.erase(it);
        }
        else
        {
            // Shrink the free block by `size`
            auto k = it->first + size;
            auto v = it->second - size;

            it = free_map.erase(it);
            it = free_map.insert(/*hint=*/it, {k, v}); // Use the `it` after erased as a hint, should be good for performance

            // It is not champion, just return
            if (k - size != hint_biggest_offset)
            {
                max_cap = hint_biggest_cap;
                return std::make_tuple(offset, max_cap, last_offset == offset);
            }

            // It is champion, need to update `scan_biggest_cap`, `scan_biggest_offset`
            // and scan other free blocks to update `biggest_offset` and `biggest_cap`
            if (v > scan_biggest_cap)
            {
                scan_biggest_cap = v;
                scan_biggest_offset = k;
            }
        }

        for (; it != free_map.end(); it++)
        {
            if (it->second > scan_biggest_cap)
            {
                scan_biggest_cap = it->second;
                scan_biggest_offset = it->first;
            }
        }
        hint_biggest_offset = scan_biggest_offset;
        hint_biggest_cap = scan_biggest_cap;

        return std::make_tuple(offset, hint_biggest_cap, last_offset == offset);
    }

    UInt64 updateAccurateMaxCapacity() override
    {
        UInt64 max_offset = 0;
        UInt64 max_cap = 0;

        for (const auto & [start, size] : free_map)
        {
            if (size > max_cap)
            {
                max_cap = size;
                max_offset = start;
            }
        }
        hint_biggest_offset = max_offset;
        hint_biggest_cap = max_cap;

        return max_cap;
    }

    bool markFreeImpl(UInt64 offset, size_t length) override
    {
        auto it = free_map.find(offset);

        /**
         * already unmarked.
         * The `offset` won't be mid of free space.
         * Because we alloc space from left to right.
         */
        if (it != free_map.end())
        {
            return true;
        }

        bool meanless = false;
        std::tie(it, meanless) = free_map.insert({offset, length});

        auto it_prev = it;
        auto it_next = it;

        /**
         * We need check current node is legal before we merge it.
         * If prev/next node exist. Check if they have overlap with the current node.
         * Also, We can’t check while doing the merge. 
         * Because it will cause the original state not to be restored
         */
        if (it != free_map.begin())
        {
            it_prev--;
            if (it_prev->first + it_prev->second > it->first)
            {
                LOG_FMT_WARNING(log, "Marked space free failed. [offset={}, size={}], prev node is [offset={},size={}]", it->first, it->second, it_prev->first, it_prev->second);
                free_map.erase(it);
                return false;
            }
        }

        it_next++;
        if (it_next != free_map.end())
        {
            if (it->first + it->second > it_next->first)
            {
                LOG_FMT_WARNING(log, "Marked space free failed. [offset={}, size={}], next node is [offset={},size={}]", it->first, it->second, it_next->first, it_next->second);
                free_map.erase(it);
                return false;
            }
        }

        /**
         * Now, we can do merge.
         * Restore the prev and next to the origin one.
         * Also, we need check begin/end again. 
         * Because there not cache result.
         */
        it_prev = it;

        // Check prev
        if (it != free_map.begin())
        {
            it_prev--;
            // Prev space can merge
            if (it_prev->first + it_prev->second == it->first)
            {
                free_map[it_prev->first] = it->first + it->second - it_prev->first;
                free_map.erase(it);
                it = it_prev;
            }

            // prev can't merge
        }

        // Check next
        it_next = it;
        it_next++;
        if (it_next == free_map.end())
        {
            return true;
        }

        if (it->first + it->second == it_next->first)
        {
            free_map[it->first] = it_next->first + it_next->second - it->first;
            free_map.erase(it_next);
        }
        // next can't merge
        return true;
    }

private:
    // Save the <offset, length> of free blocks
    std::map<UInt64, UInt64> free_map;
    // Keep a hint track of the biggest free block. Save its biggest capacity and start offset.
    // The hint could be invalid after `markSmapUsed` while restoring or `markSmapFree`.
    UInt64 hint_biggest_offset = 0;
    UInt64 hint_biggest_cap = 0;
};

using STDMapSpaceMapPtr = std::shared_ptr<STDMapSpaceMap>;

} // namespace PS::V3
} // namespace DB
