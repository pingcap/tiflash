#pragma once
#include <Common/Exception.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <fmt/format.h>

#include <ext/shared_ptr_helper.h>
#include <map>

#include "SpaceMap.h"

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace PS::V3
{
namespace details
{
// Return an iterator to the last element whose key is less than or equal to `key`.
// If no such element is found, the past-the-end iterator is returned.
template <typename C>
typename C::const_iterator
findLessEQ(const C & c, const typename C::key_type & key)
{
    auto iter = c.upper_bound(key); // first element > `key`
    // Nothing greater than key
    if (iter == c.cbegin())
        return c.cend();
    // its prev must be less than or equal to `key`
    return --iter;
}

} // namespace details
class STDMapSpaceMap
    : public SpaceMap
    , public ext::SharedPtrHelper<STDMapSpaceMap>
{
public:
    ~STDMapSpaceMap() override = default;

    bool check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker, size_t size) override
    {
        size_t idx = 0;
        for (const auto [offset, length] : free_map)
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
    }

    bool newSmap() override
    {
        free_map.insert({start, end});
        return true;
    }

    void freeSmap() override
    {
        // no need clear
    }

    void smapStats() override
    {
        UInt64 count = 0;

        LOG_DEBUG(log, "STD-Map entries status: ");
        for (auto it = free_map.begin(); it != free_map.end(); it++)
        {
            LOG_DEBUG(log, "  Space: " << count << " start:" << it->first << " size : " << it->second);
            count++;
        }
    }

    bool isMarkUnused(UInt64 offset, size_t length) override
    {
        auto it = details::findLessEQ(free_map, offset); // first free block <= `offset`
        if (it == free_map.end())
        {
            // No free blocks <= `offset`
            return false;
        }

        return (it->first <= offset && (it->first + it->second >= offset + length));
    }

    bool markSmapUsed(UInt64 offset, size_t length) override
    {
        auto it = details::findLessEQ(free_map, offset); // first free block <= `offset`
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
            LOG_WARNING(log, "Marked space used failed. [offset=" << offset << ", size=" << length << "] is bigger than space [offset=" << it->first << ",size=" << it->second << "]");
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
            if (((it->first + it->second) - offset) > length)
            {
                free_map.insert({offset + length, it->first + it->second - offset - length});
                free_map[it->first] = offset - it->first;
            }
            else
            { // < length
                free_map[it->first] = it->first + it->second - offset;
            }
        }

        return true;
    }

    std::pair<UInt64, UInt64> searchSmapInsertOffset(size_t size) override
    {
        UInt64 offset = UINT64_MAX;
        UInt64 max_cap = 0;
        // The biggest free block capacity and its start offset
        UInt64 scan_biggest_cap = 0;
        UInt64 scan_biggest_offset = 0;

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
            LOG_ERROR(log, "Not sure why can't found any place to insert. [size=" << size << "] [old biggest_offset=" << biggest_offset << "] [old biggest_cap=" << biggest_cap << "] [new biggest_offset=" << scan_biggest_offset << "] [new biggest_cap=" << scan_biggest_cap << "]");
            biggest_offset = scan_biggest_offset;
            biggest_cap = scan_biggest_cap;

            return std::make_pair(offset, max_cap);
        }

        // Update return start
        offset = it->first;

        if (it->second == size)
        {
            // It is not champion, just return
            if (it->first != biggest_offset)
            {
                free_map.erase(it);
                max_cap = biggest_cap;
                return std::make_pair(offset, max_cap);
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
            if (k - size != biggest_offset)
            {
                max_cap = biggest_cap;
                return std::make_pair(offset, max_cap);
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
        biggest_offset = scan_biggest_offset;
        biggest_cap = scan_biggest_cap;

        return std::make_pair(offset, biggest_cap);
    }

    bool markSmapFree(UInt64 offset, size_t length) override
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
                LOG_WARNING(log, "Marked space free failed. [offset=" << it->first << ", size=" << it->second << "], prev node is [offset=" << it_prev->first << ",size=" << it_prev->second << "]");
                free_map.erase(it);
                return false;
            }
        }

        it_next++;
        if (it_next != free_map.end())
        {
            if (it->first + it->second > it_next->first)
            {
                LOG_WARNING(log, "Marked space free failed. [offset=" << it->first << ", size=" << it->second << "], next node is [offset=" << it_next->first << ",size=" << it_next->second << "]");
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
    // Keep track of the biggest free block. Save its biggest capacity and start offset.
    UInt64 biggest_offset = 0;
    UInt64 biggest_cap = 0;
};

using STDMapSpaceMapPtr = std::shared_ptr<STDMapSpaceMap>;

} // namespace PS::V3
} // namespace DB
