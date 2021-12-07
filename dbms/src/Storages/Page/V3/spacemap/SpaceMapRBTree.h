#pragma once
#include <Common/Exception.h>

#include <ext/shared_ptr_helper.h>

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

class RBTreeSpaceMap
    : public SpaceMap
    , public ext::SharedPtrHelper<RBTreeSpaceMap>
{
public:
    ~RBTreeSpaceMap() override
    {
        freeSmap();
    }

    bool check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker, size_t size) override;

protected:
    RBTreeSpaceMap(UInt64 start, UInt64 end)
        : SpaceMap(start, end, SMAP64_RBTREE)
    {
    }

    bool newSmap() override;

    void freeSmap() override;

    void smapStats() override;

    bool isSmapMarkUsed(UInt64 block, size_t num) override;

    bool markSmapUsed(UInt64 block, size_t num) override;

    bool markSmapFree(UInt64 block, size_t num) override;

    std::pair<UInt64, UInt64> searchSmapInsertOffset(size_t size) override;

private:
    struct rb_private * rb_tree;
    UInt64 biggest_range = 0;
    UInt64 biggest_cap = 0;
};

using RBTreeSpaceMapPtr = std::shared_ptr<RBTreeSpaceMap>;

} // namespace PS::V3
} // namespace DB
