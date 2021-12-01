#pragma once
#include <Core/Types.h>
#include <common/logger_useful.h>

namespace DB::PS::V3
{
class SpaceMap;
using SpaceMapPtr = std::shared_ptr<SpaceMap>;
class SpaceMap
{
public:
    enum SpaceMapType
    {
        SMAP64_INVALID = 0,
        SMAP64_RBTREE = 1,
        SMAP64_STD_MAP = 2
    };

    static SpaceMapPtr createSpaceMap(SpaceMapType type, UInt64 start, UInt64 end, int cluster_bits = 0);

    /**
     * ret value :
     *  -1: Invalid args
     *   0: Unmark the marked node
     *   1: Unmark the unmarked node
     */
    int unmarkRange(UInt64 offset, size_t length);

    /**
     * ret value :
     *  -1: Invalid args
     *   1: Mark success
     */
    int markRange(UInt64 offset, size_t length);

    /**
     * ret value :
     *  -1: Invalid args
     *   0: This bit is marked
     *   1: This bit is not marked
     */
    int testRange(UInt64 offset, size_t length);

    /**
     * Search range, return the free bits 
     */
    virtual void searchRange(size_t size, UInt64 * ret, UInt64 * max_cap) = 0;

    /**
     * Clear all ranges
     */
    void clear();

    /**
     * Log the status of space map
     */
    void logStats();

    SpaceMapType getType()
    {
        return type;
    }

#ifndef DBMS_PUBLIC_GTEST
protected:
#endif

    SpaceMap(UInt64 start, UInt64 end, int cluster_bits = 0);

    virtual ~SpaceMap(){};

    /* Generic space map operators */
    virtual int newSmap() = 0;

    /* The difference between clear and free is that after you clear, you can still use this spacemap. */
    virtual void clearSmap() = 0;

    virtual void freeSmap() = 0;

    virtual int copySmap(SpaceMap * dest) = 0;

    virtual int resizeSmap(UInt64 new_end, UInt64 new_real_end) = 0;

    /* Print space maps status  */
    virtual void smapStats() = 0;

    /* Space map bit/bits test operators */
    virtual int testSmapRange(UInt64 offset, size_t size) = 0;

    /* Space map range set/unset operators */
    virtual int markSmapRange(UInt64 offset, size_t size) = 0;

    virtual int unmarkSmapRange(UInt64 offset, size_t size) = 0;

private:
    /* shift block to the right range */
    std::pair<UInt64, size_t> shiftBlock(UInt64 offset, size_t num);

    /* Check the range */
    bool checkRange(UInt64 offset, size_t num);

    /* Check Space Map have been inited or not*/
    bool checkInited();
#ifndef DBMS_PUBLIC_GTEST
protected:
#else
public:
#endif
    SpaceMapType type = SpaceMapType::SMAP64_INVALID;

    /* [left - right] range */
    UInt64 start, end;

    Poco::Logger * log;

private:
    /* shift */
    int cluster_bits;

    char * description;
};


} // namespace DB::PS::V3
