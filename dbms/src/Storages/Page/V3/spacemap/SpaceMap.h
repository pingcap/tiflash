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

    /**
     * Create a SpaceMapPtr.
     *  - type : 
     *      - SMAP64_RBTREE : red-black tree Implementation
     *      - SMAP64_STD_MAP: std::map Implementation
     *  - start : begin of the range
     *  - end : end if the range
     *  - cluster_bits : the shift mask.
     *      final range will Divide 2^cluster_bits in all public api
     *      ex. request [offset=100,size=500]
     *      cluster_bits=0: [offset=100,size=500]
     *      cluster_bits=1: [offset=100,size=500]
     * 
     */
    static SpaceMapPtr createSpaceMap(SpaceMapType type, UInt64 start, UInt64 end, int cluster_bits = 0);

    /**
     * Unmark a range [offset,offset + length) of the space map.
     * It means this range will be marked as freed.
     * After unmark this range. 
     * When user use `searchRange` to get a range.
     * Then this range may be selected(If request size fit range size).
     * 
     * ret value :
     *  -1: Invalid args
     *   0: Unmark the marked node
     *   1: Unmark the unmarked node
     */
    int unmarkRange(UInt64 offset, size_t length);

    /**
     * Mark a range [offset,offset + length) of the space map.
     * It means this range will be marked as used.
     * After this range been marked. 
     * When user use `searchRange` to get a range. 
     * This range won't be selected.
     * 
     * ret value :
     *  -1: Invalid args
     *   1: Mark success
     */
    int markRange(UInt64 offset, size_t length);

    /**
     * Test a range [offset,offset + length) have been used or not.
     * 
     * ret value :
     *  -1: Invalid args
     *   0: This range have been marked, or some sub range have been marked
     *   1: This range have not been marked, all of range is free for use.
     */
    int testRange(UInt64 offset, size_t length);

    /**
     * Search an range that can fit in `size`
     * SpaceMap will loop the range from start.
     * After it found a range which can fit this `size`.
     * It will decide if there needs to keep traverse for find `max_cap`.
     * 
     * return val:
     *  ret : offset of the range
     *  max_cap : Current SpaceMap can hold the largest size
     */
    void searchRange(size_t size, UInt64 * ret, UInt64 * max_cap);

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

    /* Free the space map if necessary */
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

    virtual void searchSmapRange(size_t size, UInt64 * ret, UInt64 * max_cap) = 0;

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

    /* The offset range managed by this SpaceMap. The range is [left, right). */
    UInt64 start;
    UInt64 end;

    Poco::Logger * log;

private:
    /* shift */
    int cluster_bits;

    char * description;
};


} // namespace DB::PS::V3
