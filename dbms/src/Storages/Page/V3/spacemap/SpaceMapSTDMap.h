#pragma once
#include <Common/Exception.h>
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
class STDMapSpaceMap
    : public SpaceMap
    , public ext::SharedPtrHelper<STDMapSpaceMap>
{
public:
<<<<<<< HEAD
    STDMapSpaceMap(UInt64 start, UInt64 end, int cluster_bits = 0)
        : SpaceMap(start, end, cluster_bits)
        , biggest_range(start)
        , biggest_cap(end - start)
=======
    ~STDMapSpaceMap() override = default;

    bool check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker, size_t size) override
>>>>>>> bak-space-map
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
        auto it = free_map.lower_bound(offset);

        if (it == free_map.end())
        {
            it--;
        }
        else if (it->first > offset && it != free_map.begin())
        {
            it--;
        }

        return (it->first <= offset && (it->first + it->second >= offset + length));
    }

    bool markSmapUsed(UInt64 offset, size_t length) override
    {
        auto it = free_map.upper_bound(offset);
        if (it == free_map.begin())
        {
            return false;
        }

        --it;

        // already been marked used
        if (it->first + it->second < offset)
        {
            return false;
        }

        if (length > it->second || it->first + it->second < offset + length)
        {
            LOG_WARNING(log, "Marked space used failed. [offset = " << offset << ", size= " << length << "] is bigger than space [offset=" << it->first << ",size=" << it->second << "]");
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
                // Shrink the free block
                auto shrink_offset = it->first + length;
                auto shrink_size = it->second - length;
                free_map.erase(it);
                free_map[shrink_offset] = shrink_size;
            }
        }
        else if (it->first + it->second == offset + length)
        {
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

<<<<<<< HEAD
    void searchRange(size_t size, UInt64 * ret, UInt64 * max_cap) override
    {
        UInt64 _biggest_cap = 0;
        UInt64 _biggest_range = 0;

        auto it = map.begin();
        for (; it != map.end(); it++)
        {
            if (it->second >= size)
            {
                break;
            }
            else
            {
                if (it->second > _biggest_cap)
                {
                    _biggest_cap = it->second;
                    _biggest_range = it->first;
                }
            }
        }

        // not place found.
        if (it == map.end())
        {
            LOG_ERROR(log, "Not sure why can't found any place to insert.[old biggest_range= " << biggest_range << "] [old biggest_cap=" << biggest_cap << "] [new biggest_range=" << _biggest_range << "] [new biggest_cap=" << _biggest_cap << "]");
            biggest_range = _biggest_range;
            biggest_cap = _biggest_cap;

            *ret = UINT64_MAX;
            *max_cap = 0;
            return;
        }

        // Update return start
        *ret = it->first;

        if (it->second == size)
        {
            // It is champion, need update
            if (it->first == biggest_range)
            {
                auto it_cur = it++;
                map.erase(it_cur);
                goto go_on_update_biggest;
            }
            else // It not champion, just return
            {
                map.erase(it);
                *max_cap = biggest_cap;
                return;
            }
        }
        else
        {
            auto k = it->first + size;
            auto v = it->second - size;

            map.erase(it);
            map.insert({k, v});

            // It is champion, need update
            if (k - size == biggest_range)
            {
                if (v > _biggest_cap)
                {
                    _biggest_cap = v;
                    _biggest_range = k;
                }
                it = map.find(k);
                goto go_on_update_biggest;
            }
            else // It not champion, just return
            {
                *max_cap = biggest_cap;
                return;
            }
        }

    go_on_update_biggest:
        for (; it != map.end(); it++)
        {
            if (it->second > _biggest_cap)
            {
                _biggest_cap = it->second;
                _biggest_range = it->first;
            }
        }
        biggest_range = _biggest_range;
        biggest_cap = _biggest_cap;
        *max_cap = biggest_cap;
    }
=======
    std::pair<UInt64, UInt64> searchSmapInsertOffset(size_t size) override
    {
        UInt64 offset = UINT64_MAX;
        UInt64 max_cap = 0;
        // The biggest free block capacity and its start offset
        UInt64 scan_biggest_cap = 0;
        UInt64 scan_biggest_offset = 0;
>>>>>>> bak-space-map

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
            // It is champion, need update
            if (it->first == biggest_offset)
            {
                it = free_map.erase(it);
                // Still need search for max_cap
            }
            else // It is not champion, just return
            {
                free_map.erase(it);
                max_cap = biggest_cap;
                return std::make_pair(offset, max_cap);
            }
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
<<<<<<< HEAD
                map.erase(it);
            }
            else // in the range
=======
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
>>>>>>> bak-space-map
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
<<<<<<< HEAD
         * The `block` won't be mid of free range.
         * Because we alloc space from left to right.
         */
        if (it != map.end())
=======
         * The `offset` won't be mid of free space.
         * Because we alloc space from left to right.
         */
        if (it != free_map.end())
>>>>>>> bak-space-map
        {
            return true;
        }

        bool meanless = false;
        std::tie(it, meanless) = free_map.insert({offset, length});

        /**
         * Not sure why clang got warning 
         * If init `it_prev` after `goto` op
         */
        auto it_prev = it;
<<<<<<< HEAD
=======
        auto it_next = it;
>>>>>>> bak-space-map

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
                LOG_WARNING(log, "Marked space free failed. [offset = " << it->first << ", size= " << it->second << "], prev node is [offset=" << it_prev->first << ",size=" << it_prev->second << "]");
                free_map.erase(it);
                return false;
            }
        }

<<<<<<< HEAD
        /**
         * but don't make this line upper, 
         * it make no sense to upper it.
         */
        it_prev--;

        // Prev range can merge
        if (it_prev->first + it_prev->second >= it->first)
=======
        it_next++;
        if (it_next != free_map.end())
>>>>>>> bak-space-map
        {
            if (it->first + it->second > it_next->first)
            {
                LOG_WARNING(log, "Marked space free failed. [offset = " << it->first << ", size= " << it->second << "], next node is [offset=" << it_next->first << ",size=" << it_next->second << "]");
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
<<<<<<< HEAD
#endif
    std::map<UInt64, UInt64> map;
    UInt64 biggest_range = 0;
=======
    // Save the <offset, length> of free blocks
    std::map<UInt64, UInt64> free_map;
    // Keep track of the biggest free block. Save its biggest capacity and start offset.
    UInt64 biggest_offset = 0;
>>>>>>> bak-space-map
    UInt64 biggest_cap = 0;
};

using STDMapSpaceMapPtr = std::shared_ptr<STDMapSpaceMap>;

} // namespace PS::V3
} // namespace DB
