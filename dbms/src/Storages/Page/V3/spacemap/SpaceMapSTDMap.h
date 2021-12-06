#pragma once
#include <Common/Exception.h>

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
class STDMapSpaceMap : public SpaceMap
{
public:
    STDMapSpaceMap(UInt64 start, UInt64 end, int cluster_bits = 0)
        : SpaceMap(start, end, cluster_bits)
        , biggest_range(start)
        , biggest_cap(end - start)
    {
        type = SMAP64_STD_MAP;
    };

    ~STDMapSpaceMap() override{

    };
#ifndef DBMS_PUBLIC_GTEST
protected:
#endif
    int newSmap() override
    {
        map.insert({start, end});
        return 0;
    }

    void clearSmap() override
    {
        map.clear();
        map.insert({start, end});
    }

    void freeSmap() override
    {
        clearSmap();
    }

    int copySmap([[maybe_unused]] SpaceMap * dest) override
    {
        throw Exception("Unimplement here. After need use, then implement it.", ErrorCodes::NOT_IMPLEMENTED);
    }

    int resizeSmap([[maybe_unused]] UInt64 new_end, [[maybe_unused]] UInt64 new_real_end) override
    {
        throw Exception("Unimplement here. After need use, then implement it.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void smapStats() override
    {
        UInt64 count = 0;

        LOG_DEBUG(log, "entry status :");
        for (auto it = map.begin(); it != map.end(); it++)
        {
            LOG_DEBUG(log, "  range : " << count << " start:" << it->first << " size : " << it->second);
            count++;
        }
    }

    int testSmapRange(UInt64 block, size_t num) override
    {
        for (auto it = map.begin(); it != map.end(); it++)
        {
            // block in the range
            if (it->first <= block && (it->first + it->second) > block)
            {
                // end of block still in the range
                return (it->first + it->second) >= (num + block);
            }

            if (it->first >= block)
            {
                break;
            }
        }

        return 0;
    }

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

    int markSmapRange(UInt64 block, size_t num) override
    {
        auto it = map.find(block);
        if (it == map.end())
        {
            // can't found , check the near one.
            for (it = map.begin(); it != map.end(); it++)
            {
                // In the range, jump to range.
                if (it->first <= block && (it->first + it->second) > block)
                {
                    goto found_range;
                }

                // Counld not found, break.
                if (it->first > block)
                {
                    break;
                }
            }
        }
        else
        {
        found_range:
            // match
            if (it->first == block)
            {
                map.erase(it);
            }
            else // in the range
            {
                // In the mid, and not match the left or right.
                // Split to two range
                if (((it->first + it->second) - block) > num)
                {
                    map.insert({block + num, it->first + it->second - block - num});
                    map[it->first] = block - it->first;
                }
                else
                { // < num
                    map[it->first] = it->first + it->second - block;
                }
            }
        }
        return 1;
    }

    int unmarkSmapRange(UInt64 block, size_t num) override
    {
        auto it = map.find(block);

        /**
         * already unmarked.
         * The `block` won't be mid of free range.
         * Because we alloc space from left to right.
         */
        if (it != map.end())
        {
            return 0;
        }

        bool meanless = false;
        std::tie(it, meanless) = map.insert({block, num});

        /**
         * Not sure why clang got warning 
         * If init `it_prev` after `goto` op
         */
        auto it_prev = it;

        if (it == map.begin())
        {
            goto only_do_right;
        }

        /**
         * but don't make this line upper, 
         * it make no sense to upper it.
         */
        it_prev--;

        // Prev range can merge
        if (it_prev->first + it_prev->second >= it->first)
        {
            map[it_prev->first] = it->first + it->second - it_prev->first;
            map.erase(it);
            it = it_prev;
        }

    only_do_right:
        if (it == map.end())
        {
            return 0;
        }

        auto it_next = it;
        it_next++;

        // next range can merge
        if (it->first + it->second >= it_next->first)
        {
            map[it->first] = it_next->first + it_next->second - it->first;
            map.erase(it_next);
        }

        return 0;
    }
#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    std::map<UInt64, UInt64> map;
    UInt64 biggest_range = 0;
    UInt64 biggest_cap = 0;
};

using STDMapSpaceMapPtr = std::shared_ptr<STDMapSpaceMap>;

} // namespace PS::V3
} // namespace DB
