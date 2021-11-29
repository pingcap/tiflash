#pragma once
#include <map>

#include "SpaceMap.h"


namespace DB::PS::V3
{
class STDMapSpaceMap : public SpaceMap
{
public:
    STDMapSpaceMap()
    {
        type = SMAP64_STD_MAP;
    };

    virtual ~STDMapSpaceMap() override = default;

    /* Generic space map operators */
    int newSmap() override
    {
        /*
        std::map<UInt64, UInt64> * private_data;

        private_data = new std::map<UInt64, UInt64>();
        if (private_data == nullptr)
        {
            return -1;
        }

        smap->_private = (void *)private_data;
*/
        return 0;
    }

    void clearSmap() override
    {
        map.clear();
    }

    void freeSmap() override
    {
        map.clear();
    }

    int copySmap([[maybe_unused]] SpaceMap * dest) override
    {
        // TBD : no implement
        return -1;
    }

    int resizeSmap([[maybe_unused]] UInt64 new_end, [[maybe_unused]] UInt64 new_real_end) override
    {
        // TBD : no implement
        return -1;
    }

    void smapStats() override
    {
        UInt64 count = 0;

        printf("entry status :\n");
        for (auto it = map.begin(); it != map.end(); it++)
        {
            printf("range %lld : start=%lld , count=%lld \n", count, it->first, it->second);
            count++;
        }
    }

    int testSmapRange(UInt64 block, unsigned int num) override
    {
        for (auto it = map.begin(); it != map.end(); it++)
        {
            // If the range contain the search
            if (it->first < block && (it->first + it->second) > (block + num))
            {
                return 0;
            }

            // TBD
        }

        return 1;
    }

    void searchSmapRange([[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] size_t num, [[maybe_unused]] UInt64 * ret) override
    {
        // TBD
    }

    int findSmapFirstZero([[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] UInt64 * out) override
    {
        return -1;
    }

    int findSmapFirstSet([[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] UInt64 * out) override
    {
        return -1;
    }

    int markSmapRange(UInt64 block, unsigned int num) override;

    int unmarkSmapRange(UInt64 block, unsigned int num) override;

private:
    std::map<UInt64, UInt64> map;
};

} // namespace DB::PS::V3
