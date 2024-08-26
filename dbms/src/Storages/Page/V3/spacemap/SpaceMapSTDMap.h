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
#include <tuple>

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
        free_map.insert({start, end - start});
        std::set<UInt64> offsets{start};
        free_map_invert_index.emplace(end - start, offsets);
    }

    String toDebugString() override
    {
        UInt64 count = 0;

        FmtBuffer fmt_buffer;
        fmt_buffer.append("SpaceMap entries: [");
        fmt_buffer.joinStr(
            free_map.begin(),
            free_map.end(),
            [&count](const auto & it, FmtBuffer & fb) {
                fb.fmtAppend(R"({{"index":{},"start":{},"size":{}}})", count, it.first, it.second);
                count += 1;
            },
            ",");
        fmt_buffer.append("]");
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
        // An empty data, we can simply consider it is stored and return true
        // Do not let it split the space into smaller pieces.
        if (length == 0)
            return true;

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
            LOG_WARNING(
                Logger::get(),
                "Marked space used failed. [offset={}, size={}] is bigger than space [offset={},size={}]",
                offset,
                length,
                it->first,
                it->second);
            return false;
        }

        // remove it from `free_map_invert_index`
        deleteFromInvertIndex(it->second, it->first);

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
                insertIntoInvertIndex(shrink_size, shrink_offset);
                free_map[shrink_offset] = shrink_size;
            }
        }
        else if (it->first + it->second == offset + length)
        {
            // Shrink the free block from right
            assert(it->second != length); // should not run into here
            insertIntoInvertIndex(it->second - length, it->first);
            free_map[it->first] = it->second - length;
        }
        else
        {
            // In the mid, and not match the left or right.
            // Split to two space
            insertIntoInvertIndex(it->first + it->second - offset - length, offset + length);
            insertIntoInvertIndex(offset - it->first, it->first);
            free_map.insert({offset + length, it->first + it->second - offset - length});
            free_map[it->first] = offset - it->first;
        }

        return true;
    }

    // return value is <insert_offset, max_cap, is_expansion>
    std::tuple<UInt64, UInt64, bool> searchInsertOffset(size_t size) override
    {
        if (unlikely(size == 0))
        {
            // The returned `max_cap` is 0 under this case, user should not use it.
            return std::make_tuple(0, 0, false);
        }

        if (unlikely(free_map.empty()))
        {
            LOG_ERROR(Logger::get(), "Current space map is full");
            return std::make_tuple(UINT64_MAX, 0, false);
        }
        RUNTIME_CHECK_MSG(
            !free_map_invert_index.empty(),
            "Invalid state: free_map is empty but invert index is not empty");
        auto iter = free_map_invert_index.lower_bound(size);
        if (unlikely(iter == free_map_invert_index.end()))
        {
            LOG_ERROR(Logger::get(), "Can't found any place to insert for size {}", size);
            return std::make_tuple(UINT64_MAX, free_map_invert_index.rbegin()->first, false);
        }
        auto length = iter->first;
        auto offset = *(iter->second.begin());
        bool is_expansion = (offset + length == end);
        deleteFromInvertIndex(length, offset);
        assert(length >= size);
        free_map.erase(offset);
        if (length > size)
        {
            free_map.insert({offset + size, length - size});
            insertIntoInvertIndex(length - size, offset + size);
        }
        UInt64 biggest_cap = free_map_invert_index.empty() ? 0 : free_map_invert_index.rbegin()->first;
        return std::make_tuple(offset, biggest_cap, is_expansion);
    }

    UInt64 updateAccurateMaxCapacity() override
    {
        return free_map_invert_index.empty() ? 0 : free_map_invert_index.rbegin()->first;
    }

    bool markFreeImpl(UInt64 offset, size_t length) override
    {
        // for an empty blob, no new free block is created, just skip
        if (length == 0)
        {
            return true;
        }

        /**
         * already unmarked.
         * The `offset` won't be mid of free space.
         * Because we alloc space from left to right.
         */
        auto it = free_map.find(offset);
        if (it != free_map.end())
        {
            return true;
        }

        bool meanless = false;
        std::tie(it, meanless) = free_map.insert({offset, length});
        insertIntoInvertIndex(length, offset);

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
                LOG_WARNING(
                    Logger::get(),
                    "Marked space free failed. [offset={}, size={}], prev node is [offset={},size={}]",
                    it->first,
                    it->second,
                    it_prev->first,
                    it_prev->second);
                free_map.erase(it);
                deleteFromInvertIndex(length, offset);
                return false;
            }
        }

        it_next++;
        if (it_next != free_map.end())
        {
            if (it->first + it->second > it_next->first)
            {
                LOG_WARNING(
                    Logger::get(),
                    "Marked space free failed. [offset={}, size={}], next node is [offset={},size={}]",
                    it->first,
                    it->second,
                    it_next->first,
                    it_next->second);
                free_map.erase(it);
                deleteFromInvertIndex(length, offset);
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
                deleteFromInvertIndex(it_prev->second, it_prev->first);
                deleteFromInvertIndex(length, offset);
                insertIntoInvertIndex(it->first + it->second - it_prev->first, it_prev->first);
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
            deleteFromInvertIndex(it_next->second, it_next->first);
            deleteFromInvertIndex(it->second, it->first);
            insertIntoInvertIndex(it_next->first + it_next->second - it->first, it->first);
            free_map[it->first] = it_next->first + it_next->second - it->first;
            free_map.erase(it_next);
        }
        // next can't merge
        return true;
    }

private:
    inline void deleteFromInvertIndex(UInt64 length, UInt64 offset)
    {
        auto index_iter = free_map_invert_index.find(length);
        RUNTIME_CHECK_MSG(
            index_iter != free_map_invert_index.end(),
            "Fail to find length {} offset {} in free_map_invert_index",
            length,
            offset);
        auto & offsets = index_iter->second;
        offsets.erase(offset);
        if (offsets.empty())
        {
            free_map_invert_index.erase(length);
        }
    }

    inline void insertIntoInvertIndex(UInt64 length, UInt64 offset)
    {
        if (auto index_iter = free_map_invert_index.find(length); index_iter != free_map_invert_index.end())
        {
            index_iter->second.insert(offset);
        }
        else
        {
            std::set<UInt64> offsets{offset};
            free_map_invert_index.emplace(length, offsets);
        }
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif
    // Save the <offset, length> of free blocks
    std::map<UInt64, UInt64> free_map;
    // Length -> set<Offset>
    std::map<UInt64, std::set<UInt64>> free_map_invert_index;
};

using STDMapSpaceMapPtr = std::shared_ptr<STDMapSpaceMap>;

} // namespace PS::V3
} // namespace DB
