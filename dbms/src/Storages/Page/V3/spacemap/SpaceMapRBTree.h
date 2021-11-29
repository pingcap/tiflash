#pragma once
#include "RBTree.h"
#include "SpaceMap.h"


namespace DB::PS::V3 
{

struct smap_rb_entry
{
    struct rb_node node;
    UInt64 start;
    UInt64 count;
};

struct rb_private
{
    struct rb_root root;
    // Cache the index for write
    struct smap_rb_entry * write_index;
    // Cache the index for read
    struct smap_rb_entry * read_index;
    struct smap_rb_entry * read_index_next;
};

// convert rb_node to smap_rb_entry
inline static struct smap_rb_entry * node_to_entry(struct rb_node * node)
{
    struct smap_rb_entry * rb_ex;
    #define container_of(ptr, type, member) ({          \
        const __typeof__( ((type *)0)->member ) *__mptr = (ptr);    \
        (type *)( (char *)__mptr - offsetof(type,member) ); })

    rb_ex = container_of(node, struct smap_rb_entry, node);

    #undef container_of
    return rb_ex;
}

class RBTreeSpaceMapOps : public SpaceMapOps 
{
public:

    RBTreeSpaceMapOps()
    {
        type = SMAP64_RBTREE;
    };

    ~RBTreeSpaceMapOps() override {};

    /* Generic space map operators */
    int newSmap(SpaceMap * smap) override;

    /* The difference between clear and free is that after you clear, you can still use this spacemap. */
    void clearSmap(SpaceMap * smap) override;

    void freeSmap(SpaceMap * smap) override;

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

    /* Print space maps status  */
    void smapStats(SpaceMap * smap) override;

    /* Space map bit/bits test operators */
    int testSmapBit(SpaceMap * smap, UInt64 block) override;

    int testSmapRange(SpaceMap * smap, UInt64 block, unsigned int num) override;

    /* Search range , return the free bits */
    void searchSmapRange([[maybe_unused]] SpaceMap * smap, [[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] size_t num,
        [[maybe_unused]] UInt64 * ret) override
    {
        // TBD
    }

    /* Find the first zero/set bit between start and end, inclusive.
     * May be NULL, in which case a generic function is used. */
    int findSmapFirstZero([[maybe_unused]] SpaceMap * smap, [[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] UInt64 * out) override
    {
        return -1;
    }

    int findSmapFirstSet([[maybe_unused]] SpaceMap * smap, [[maybe_unused]] UInt64 start, [[maybe_unused]] UInt64 end, [[maybe_unused]] UInt64 * out) override
    {
        return -1;
    }

    /* Space map bit set/unset operators */
    int markSmapBit(SpaceMap * smap, UInt64 block) override;

    int unmarkSmapBit(SpaceMap * smap, UInt64 block) override;

    /* Space map range set/unset operators */
    int markSmapRange(SpaceMap * smap, UInt64 block, unsigned int num) override;

    int unmarkSmapRange(SpaceMap * smap, UInt64 block, unsigned int num) override;
};


}