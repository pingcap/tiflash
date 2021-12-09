#pragma once
#include <Core/Types.h>
#include <common/logger_useful.h>

namespace DB::PS::V3
{
class SpaceMap;
using SpaceMapPtr = std::shared_ptr<SpaceMap>;
/**
 * SpaceMap design doc: 
 * https://docs.google.com/document/d/1l1GoIV6Rp0GEwuYtToJMKYACmZv6jf4kp1n8JdQidS8/edit#heading=h.pff0nn7vsa6w
 * 
 * SpaceMap have red-black tree/ map implemention.
 * Each node on the tree records the information of free data blocks,
 * 
 * The node is composed of `offset` : `size`. Each node sorted according to offset.
 * - offset: Record the starting address of the free data segment in the file.
 * - size: The length of the space data segment is recorded.
 */
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
     * Search a span that can fit in `size`.
     * If such span is found, it will also return a hint of the max capacity available
     * in this SpaceMap.
     * 
     * return value is <insert_offset, max_cap>:
     *  insert_offset : start offset for the inserted space
     *  max_cap : A hint of the largest available space this SpaceMap can hold. 
     */
    virtual std::pair<UInt64, UInt64> searchInsertOffset(size_t size) = 0;

    /**
     * Sanity check for correctness
     */
    using CheckerFunc = std::function<bool(size_t idx, UInt64 start, UInt64 end)>;
    virtual bool check(CheckerFunc /*checker*/, size_t /*size*/)
    {
        return true;
    }

    /**
     * Log the status of space map
     */
    void logStats();

    SpaceMapType getType() const
    {
        return type;
    }

    static String typeToString(SpaceMapType type)
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

protected:
    SpaceMap(UInt64 start_, UInt64 end_, SpaceMapType type_);

    virtual ~SpaceMap() = default;

    /* Print space maps status  */
    virtual void smapStats() = 0;

    // Return true if space [offset, offset+size) are all free
    virtual bool isMarkUnused(UInt64 offset, size_t size) = 0;

    /* Space map mark used/free operators */
    virtual bool markUsedImpl(UInt64 offset, size_t size) = 0;

    virtual bool markFreeImpl(UInt64 offset, size_t size) = 0;

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
