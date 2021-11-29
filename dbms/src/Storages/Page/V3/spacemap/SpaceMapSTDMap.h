#pragma once
#include <map>
#include "SpaceMap.h"


namespace DB::PS::V3  {

class STDMapSpaceMapOps : public SpaceMapOps 
{
public:
    STDMapSpaceMapOps()
    {
        type = SMAP64_STD_MAP;
    };

    ~STDMapSpaceMapOps() override {};

    /* Generic space map operators */
    int newSmap(SpaceMap * smap) override
    {
        std::map<UInt64,UInt64> * private_data;

        private_data = new std::map<UInt64,UInt64>();
        if (private_data == nullptr)
        {
            return -1;
        }

        smap->_private = (void *)private_data;

        return 0;
    }

    void clearSmap(SpaceMap * smap) override
    {
        std::map<UInt64,UInt64> * map;

        map = (std::map<UInt64,UInt64> *)smap->_private;
        map->clear();
    }

    void freeSmap(SpaceMap * smap) override
    {
        std::map<UInt64,UInt64> * map;

        map = (std::map<UInt64,UInt64> *)smap->_private;
        map->clear();

        delete map;
        smap->_private = nullptr;
    }

    int copySmap([[maybe_unused]] SpaceMap * src, [[maybe_unused]] SpaceMap * dest) override
    {
        // TBD : no implement
        return -1;
    }

    int resizeSmap([[maybe_unused]] SpaceMap * smap, [[maybe_unused]] UInt64 new_end, [[maybe_unused]] UInt64 new_real_end) override
    {
        // TBD : no implement
        return -1;
    }

    void smapStats(SpaceMap * smap) override
    {
        std::map<UInt64,UInt64> * map;

        map = (std::map<UInt64,UInt64> *)smap->_private;
        UInt64 count = 0;

        printf("entry status :\n");
        for (auto it = map->begin(); it != map->end(); it++)
        {
            printf("range %lld : start=%lld , count=%lld \n", count, it->first, it->second);
            count++;
        }
    }

    int testSmapBit(SpaceMap * smap, UInt64 block) override
    {
        std::map<UInt64,UInt64> * map;

        map = (std::map<UInt64,UInt64> *)smap->_private;
        auto it = map->find(block);
        return it == map->end();
    }

    int testSmapRange(SpaceMap * smap, UInt64 block, unsigned int num) override
    {
        std::map<UInt64,UInt64> * map;
        map = (std::map<UInt64,UInt64> *)smap->_private;

        for (auto it = map->begin(); it != map->end(); it++)
        {
            // If the range contain the search
            if (it->first <  block && (it->first + it->second) > (block + num))
            {
                return 0;
            }

            // TBD    

        }

        return 1;
    }

    void searchSmapRange([[maybe_unused]] SpaceMap * smap, [[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] size_t num,
        [[maybe_unused]] UInt64 * ret) override
    {
        // TBD
    }

    int findSmapFirstZero([[maybe_unused]] SpaceMap * smap, [[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] UInt64 * out) override
    {
        return -1;
    }

    int findSmapFirstSet([[maybe_unused]] SpaceMap * smap, [[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] UInt64 * out) override
    {
        return -1;
    }


    int markSmapBit(SpaceMap * smap, UInt64 block) override;

    int unmarkSmapBit(SpaceMap * smap, UInt64 block) override;

    int markSmapRange(SpaceMap * smap, UInt64 block, unsigned int num) override;

    int unmarkSmapRange(SpaceMap * smap, UInt64 block, unsigned int num) override;
};

}