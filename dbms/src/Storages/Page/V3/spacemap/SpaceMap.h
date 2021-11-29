#pragma once
#include <Core/Types.h>

namespace DB::PS::V3 
{

#define SMAP64_INVALID 0
#define SMAP64_RBTREE 1
#define SMAP64_STD_MAP 2

class SpaceMap
{
private:
    int flags;
    /* [left - right] range */
    UInt64 start, end;
    /* limit range */
    UInt64 real_end;
    /* shift */
    int cluster_bits;
    
    char * description;

public:
    SpaceMap(UInt64 start, UInt64 end, UInt64 real_end);

    virtual ~SpaceMap();

    void printf();

    /**
     * The return value same as `unmark`
     */
    int unmarkRange(UInt64 offset, size_t length);

    /**
     * The return value same as `mark`
     */
    int markRange(UInt64 offset, size_t length);

    /**
     * The return value same as `test`
     */
    int testRange(UInt64 offset, size_t length);

protected:

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
    virtual int testSmapRange(UInt64 block, unsigned int num) = 0;

    /* Search range, return the free bits */
    virtual void searchSmapRange(UInt64 start, UInt64 end, size_t num, UInt64 * ret) = 0;

    /* Find the first zero/set bit between start and end, inclusive.
     * May be NULL, in which case a generic function is used. */
    virtual int findSmapFirstZero(UInt64 start, UInt64 end, UInt64 * out) = 0;

    virtual int findSmapFirstSet(UInt64 start, UInt64 end, UInt64 * out) = 0;

    /* Space map range set/unset operators */
    virtual int markSmapRange(UInt64 block, unsigned int num) = 0;

    virtual int unmarkSmapRange(UInt64 block, unsigned int num) = 0;

};

}
