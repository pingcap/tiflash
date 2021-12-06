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
     * Create a SpaceMap that manages space address [start, end).
     *  - type : 
     *      - SMAP64_RBTREE : red-black tree implementation
     *      - SMAP64_STD_MAP: std::map implementation
     *  - start : begin of the space
     *  - end : end if the space
     */
    static SpaceMapPtr createSpaceMap(SpaceMapType type, UInt64 start, UInt64 end);

    /**
     * Mark a space [offset,offset + length) free of the space map.
     * After mark this space freed.
     * When user use `searchInsertOffset` to get a space.
     * Then this space may been selected(If request size fit space size
     * and it is the first freed space).
     * 
     * ret value:
     *   true: mark the space which is used.
     *   false: mark the space which is freed.
     */
    bool markFree(UInt64 offset, size_t length);

    /**
     * Mark a space [offset,offset + length) of the space map.
     * After this space been marked.
     * When user use `searchInsertOffset` to get a space. 
     * This space won't be selected.
     * ret value:
     *   false: This space is freed, marked all space used.
     *   true: This space is used, or some sub space is used.
     */
    bool markUsed(UInt64 offset, size_t length);

    /**
     * Test a space [offset,offset + length) have been used or not.
     * 
     * ret value:
     *   true: This space is used, or some sub space is used
     *   false: This space is freed, all of space is freed for use.  
     */
    bool isMarkUsed(UInt64 offset, size_t length);

    /**
     * Search a space that can fit in `size`
     * SpaceMap will loop the range from start.
     * After it found a range which can fit this `size`.
     * It will decide if there needs to keep traverse to update `max_cap`.
     * 
     * Return value is <insert_offset, max_cap>:
     *  insert_offset : start offset for the inserted space
     *  max_cap : The largest available space this SpaceMap can hold. 
     */
    std::pair<UInt64, UInt64> searchInsertOffset(size_t size);

    /**
     * Log the status of space map
     */
    void logStats();

    SpaceMapType getType()
    {
        return type;
    }

    String typeToString(SpaceMapType type)
    {
        switch (type)
        {
        case SMAP64_RBTREE:
            return "RB-Tree";
        case SMAP64_STD_MAP:
            return "STD Map";
        default:
            return "Invalid";
        }
    }

#ifndef DBMS_PUBLIC_GTEST
protected:
#endif

    SpaceMap(UInt64 start, UInt64 end);

    virtual ~SpaceMap(){};

    /* Generic space map operators */
    virtual bool newSmap() = 0;

    /* Free the space map if necessary */
    virtual void freeSmap() = 0;

    /* Print space maps status  */
    virtual void smapStats() = 0;

    /* Space map bit/bits test operators */
    virtual bool isSmapMarkUsed(UInt64 offset, size_t size) = 0;

    /* Space map mark used/free operators */
    virtual bool markSmapUsed(UInt64 offset, size_t size) = 0;

    virtual bool markSmapFree(UInt64 offset, size_t size) = 0;

    virtual std::pair<UInt64, UInt64> searchSmapInsertOffset(size_t size) = 0;

private:
    /* Check the range */
    bool checkSpace(UInt64 offset, size_t num);

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
};


} // namespace DB::PS::V3
