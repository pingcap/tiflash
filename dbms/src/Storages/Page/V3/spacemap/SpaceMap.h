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
     * Mark a span [offset,offset + length) to be free.
     * After this span is marked free, this span may be selected by `searchInsertOffset`.
     * 
     * ret value:
     *   true: the span is marked as free
     *   false: the span can not mark as free
     */
    bool markFree(UInt64 offset, size_t length);

    /**
     * Mark a span [offset,offset + length) to being used.
     * After this span is marked used, this span can not be selected by `searchInsertOffset`.
     *
     * ret value:
     *   false: This span is marked as used successfully.
     *   true: This span can not be marked as used. It or some sub spans have been marked as used before.
     */
    bool markUsed(UInt64 offset, size_t length);

    /**
     * Check a span [offset, offset + length) has been used or not.
     * 
     * ret value:
     *   true: This span is used, or some sub span is used
     *   false: All of this span is freed.
     */
    bool isMarkUsed(UInt64 offset, size_t length);

    /**
     * Search a span that can fit in `size`.
     * If such span is found.
     * It will mark that span to be used and also return a hint of the max capacity available in this SpaceMap. 
     * 
     * return value is <insert_offset, max_cap>:
     *  insert_offset : start offset for the inserted space
     *  max_cap : A hint of the largest available space this SpaceMap can hold. 
     */
    virtual std::pair<UInt64, UInt64> searchInsertOffset(size_t size) = 0;

    /**
     * Get the offset of the last free block. `[margin_offset, +∞)` is not used at all.
     */
    virtual UInt64 getRightMargin() = 0;

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
