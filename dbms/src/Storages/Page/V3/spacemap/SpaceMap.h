#pragma once
#include <Core/Types.h>

namespace DB::PS::V3 
{

#define SMAP64_INVALID 0
#define SMAP64_RBTREE 1
#define SMAP64_STD_MAP 2

struct SpaceMap;

class SpaceMapOps
{
public:
    virtual ~SpaceMapOps() = default;

    /* Generic space map operators */
    virtual int newSmap(SpaceMap * smap) = 0;

    /* The difference between clear and free is that after you clear, you can still use this spacemap. */
    virtual void clearSmap(SpaceMap * smap) = 0;

    virtual void freeSmap(SpaceMap * smap) = 0;

    virtual int copySmap(SpaceMap * src, SpaceMap * dest) = 0;

    virtual int resizeSmap(SpaceMap * smap, UInt64 new_end, UInt64 new_real_end) = 0;

    /* Print space maps status  */
    virtual void smapStats(struct SpaceMap * smap) = 0;

    /* Space map bit/bits test operators */
    virtual int testSmapBit(struct SpaceMap * smap, UInt64 block) = 0;

    virtual int testSmapRange(struct SpaceMap * smap, UInt64 block, unsigned int num) = 0;

    /* Search range , return the free bits */
    virtual void searchSmapRange(struct SpaceMap * smap, UInt64 start, UInt64 end, size_t num, UInt64 * ret) = 0;

    /* Find the first zero/set bit between start and end, inclusive.
     * May be NULL, in which case a generic function is used. */
    virtual int findSmapFirstZero(struct SpaceMap * smap, UInt64 start, UInt64 end, UInt64 * out) = 0;

    virtual int findSmapFirstSet(struct SpaceMap * smap, UInt64 start, UInt64 end, UInt64 * out) = 0;

    /* Space map bit set/unset operators */
    virtual int markSmapBit(struct SpaceMap * smap, UInt64 block) = 0;

    virtual int unmarkSmapBit(struct SpaceMap * smap, UInt64 block) = 0;

    /* Space map range set/unset operators */
    virtual int markSmapRange(struct SpaceMap * smap, UInt64 block, unsigned int num) = 0;

    virtual int unmarkSmapRange(struct SpaceMap * smap, UInt64 block, unsigned int num) = 0;

    /* Get Space map type */
    int getSmapType()
    {
        return type;
    }

protected:
    int type = SMAP64_INVALID;
};

using SpaceMapOpsPtr = std::shared_ptr<SpaceMapOps>;

struct SpaceMap
{
public:
    SpaceMapOpsPtr spacemap_ops;

    int flags;
    /* [left - right] range */
    UInt64 start, end;
    /* limit range */
    UInt64 real_end;
    /* shift */
    int cluster_bits;
    
    char * description;
    void * _private;

    SpaceMap(SpaceMapOpsPtr ops, UInt64 start, UInt64 end, UInt64 real_end);

    ~SpaceMap();

    void printf();

    /**
     * ret value :
     *  -1: Invalid args
     *   0: Unmark the marked node
     *   1: Unmark the unmarked node
     */
    int unmark(UInt64 arg);

    /**
     * ret value :
     *  -1: Invalid args
     *   1: Mark success
     */
    int mark(UInt64 arg);

    /**
     * The return value same as `unmark`
     */
    int unmarkRange(UInt64 block, unsigned int num);

    /**
     * The return value same as `mark`
     */
    int markRange(UInt64 block, unsigned int num);

    /**
     * ret value :
     *  -1: Invalid args
     *   0: This bit is marked
     *   1: This bit is not marked
     */
    int test(UInt64 arg);

    /**
     * The return value same as `test`
     */
    int testRange(UInt64 block, unsigned int num);

};

}
