#pragma once
#include <Common/Exception.h>

#include "RBTree.h"
#include "SpaceMap.h"

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace PS::V3
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

class RBTreeSpaceMap : public SpaceMap
{
public:
    RBTreeSpaceMap(UInt64 start, UInt64 end, int cluster_bits = 0)
        : SpaceMap(start, end, cluster_bits)
        , biggest_range(start)
        , biggest_cap(end - start)
    {
        type = SMAP64_RBTREE;
    };

    ~RBTreeSpaceMap() override
    {
        freeSmap();
    };
#ifndef DBMS_PUBLIC_GTEST
protected:
#endif
    /* Generic space map operators */
    int newSmap() override;

    /* The difference between clear and free is that after you clear, you can still use this spacemap. */
    void clearSmap() override;

    void freeSmap() override;

    int copySmap([[maybe_unused]] SpaceMap * dest) override
    {
        throw Exception("Unimplement here. After need use, then implement it.", ErrorCodes::NOT_IMPLEMENTED);
    }

    int resizeSmap([[maybe_unused]] UInt64 new_end, [[maybe_unused]] UInt64 new_real_end) override
    {
        throw Exception("NOT_IMPLEMENTED. After need use, then implement it.", ErrorCodes::NOT_IMPLEMENTED);
    }

    /* Print space maps status  */
    void smapStats() override;

    /* Space map bit/bits test operators */
    int testSmapRange(UInt64 block, size_t num) override;

    /* Search range , return the free bits */
    void searchRange(size_t size, UInt64 * ret, UInt64 * max_cap) override;

    int markSmapRange(UInt64 block, size_t num) override;

    int unmarkSmapRange(UInt64 block, size_t num) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    struct rb_private * rb_tree;
    UInt64 biggest_range = 0;
    UInt64 biggest_cap = 0;
};

using RBTreeSpaceMapPtr = std::shared_ptr<RBTreeSpaceMap>;

} // namespace PS::V3
} // namespace DB