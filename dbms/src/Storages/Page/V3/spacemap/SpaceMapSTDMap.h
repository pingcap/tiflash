#pragma once
#include <Common/Exception.h>

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
    ~STDMapSpaceMap() override = default;

    bool check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker) override
    {
        size_t idx = 0;
        for (const auto [offset, length] : free_map)
        {
            if (!checker(idx, offset, offset + length))
                return false;
            idx++;
        }
        return true;
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

        LOG_DEBUG(log, "entry status :");
        for (auto it = free_map.begin(); it != free_map.end(); it++)
        {
            LOG_DEBUG(log, "  Space : " << count << " start:" << it->first << " size : " << it->second);
            count++;
        }
    }

    bool isSmapMarkUsed(UInt64 block, size_t num) override
    {
        for (auto it = free_map.begin(); it != free_map.end(); it++)
        {
            // block in the space
            if (it->first <= block && (it->first + it->second) > block)
            {
                // end of block still in the space
                return (it->first + it->second) >= (num + block);
            }

            if (it->first >= block)
            {
                break;
            }
        }

        return false;
    }

    bool markSmapUsed(UInt64 block, size_t num) override
    {
        auto it = free_map.find(block);
        if (it == free_map.end())
        {
            // can't found , check the near one.
            for (it = free_map.begin(); it != free_map.end(); it++)
            {
                // In the space, jump to space.
                if (it->first <= block && (it->first + it->second) > block)
                {
                    break;
                }

                // Could not found
                if (it->first > block)
                {
                    return false;
                }
            }
        }

        // match
        if (it->first == block)
        {
            free_map.erase(it);

            // in the space
        }
        else
        {
            // In the mid, and not match the left or right.
            // Split to two space
            if (((it->first + it->second) - block) > num)
            {
                free_map.insert({block + num, it->first + it->second - block - num});
                free_map[it->first] = block - it->first;
            }
            else
            { // < num
                free_map[it->first] = it->first + it->second - block;
            }
        }

        return true;
    }

    std::pair<UInt64, UInt64> searchSmapInsertOffset(size_t size) override
    {
        UInt64 offset = UINT64_MAX;
        UInt64 max_cap = 0;
        UInt64 _biggest_cap = 0;
        UInt64 _biggest_range = 0;

        auto it = free_map.begin();
        for (; it != free_map.end(); it++)
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
        if (it == free_map.end())
        {
            LOG_ERROR(log, "Not sure why can't found any place to insert.[old biggest_range= " << biggest_range << "] [old biggest_cap=" << biggest_cap << "] [new biggest_range=" << _biggest_range << "] [new biggest_cap=" << _biggest_cap << "]");
            biggest_range = _biggest_range;
            biggest_cap = _biggest_cap;

            return std::make_pair(offset, max_cap);
        }

        // Update return start
        offset = it->first;

        if (it->second == size)
        {
            // It is champion, need update
            if (it->first == biggest_range)
            {
                auto it_cur = it++;
                free_map.erase(it_cur);
                // still need search for max_cap
            }
            else // It not champion, just return
            {
                free_map.erase(it);
                max_cap = biggest_cap;
                return std::make_pair(offset, max_cap);
            }
        }
        else
        {
            auto k = it->first + size;
            auto v = it->second - size;

            free_map.erase(it);
            free_map.insert({k, v});

            // It is champion, need update
            if (k - size == biggest_range)
            {
                if (v > _biggest_cap)
                {
                    _biggest_cap = v;
                    _biggest_range = k;
                }
                it = free_map.find(k);
                // still need search for max_cap
            }
            else // It not champion, just return
            {
                max_cap = biggest_cap;
                return std::make_pair(offset, max_cap);
            }
        }

        for (; it != free_map.end(); it++)
        {
            if (it->second > _biggest_cap)
            {
                _biggest_cap = it->second;
                _biggest_range = it->first;
            }
        }
        biggest_range = _biggest_range;
        biggest_cap = _biggest_cap;
        max_cap = biggest_cap;

        return std::make_pair(offset, max_cap);
    }

    bool markSmapFree(UInt64 block, size_t num) override
    {
        auto it = free_map.find(block);

        /**
         * already unmarked.
         * The `block` won't be mid of free space.
         * Because we alloc space from left to right.
         */
        if (it != free_map.end())
        {
            return true;
        }

        bool meanless = false;
        std::tie(it, meanless) = free_map.insert({block, num});

        auto it_prev = it;

        if (it != free_map.begin())
        {
            it_prev--;

            // Prev space can merge
            if (it_prev->first + it_prev->second >= it->first)
            {
                free_map[it_prev->first] = it->first + it->second - it_prev->first;
                free_map.erase(it);
                it = it_prev;
            }
        }

        // Check right
        if (it == free_map.end())
        {
            return false;
        }

        auto it_next = it;
        it_next++;

        // next space can merge
        if (it->first + it->second >= it_next->first)
        {
            free_map[it->first] = it_next->first + it_next->second - it->first;
            free_map.erase(it_next);
        }

        return false;
    }

private:
    // Save the <offset, length> of free blocks
    std::map<UInt64, UInt64> free_map;
    UInt64 biggest_range = 0;
    UInt64 biggest_cap = 0;
};

using STDMapSpaceMapPtr = std::shared_ptr<STDMapSpaceMap>;

} // namespace PS::V3
} // namespace DB
