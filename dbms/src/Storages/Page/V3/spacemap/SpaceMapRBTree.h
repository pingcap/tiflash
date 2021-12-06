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
    return reinterpret_cast<smap_rb_entry *>(node);
}

class RBTreeSpaceMap : public SpaceMap
{
public:
    RBTreeSpaceMap(UInt64 start, UInt64 end, int cluster_bits = 0)
        : SpaceMap(start, end, cluster_bits)
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
    int newSmap() override;

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
    void searchSmapRange([[maybe_unused]] size_t size, [[maybe_unused]] UInt64 * ret, [[maybe_unused]] UInt64 * max_cap) override
    {
        // Will implement in BlobStore
    }

    int markSmapRange(UInt64 block, size_t num) override;

    int unmarkSmapRange(UInt64 block, size_t num) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    struct rb_private * rb_tree;
};

using RBTreeSpaceMapPtr = std::shared_ptr<RBTreeSpaceMap>;

} // namespace PS::V3
} // namespace DB