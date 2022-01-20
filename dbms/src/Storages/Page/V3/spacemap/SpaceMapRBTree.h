#pragma once
#include <Common/Exception.h>
#include <Storages/Page/V3/spacemap/RBTree.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>

#include <ext/shared_ptr_helper.h>

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
{
public:
    ~RBTreeSpaceMap() override
    {
        freeSmap();
    }

    bool check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker, size_t size) override;

    static std::shared_ptr<RBTreeSpaceMap> create(UInt64, UInt64 end);

    std::pair<UInt64, UInt64> searchInsertOffset(size_t size) override;

    std::pair<UInt64, UInt64> getSizes() const override
    {
        struct rb_node * node = rb_tree_last(&rb_tree->root);
        if (node == nullptr)
        {
            auto range = end - start;
            return std::make_pair(range, range);
        }

        auto * entry = node_to_entry(node);
        UInt64 total_size = entry->start - start;
        UInt64 last_node_size = entry->count;
        UInt64 valid_size = 0;

        for (node = rb_tree_first(&rb_tree->root); node != nullptr; node = rb_tree_next(node))
        {
            entry = node_to_entry(node);
            valid_size += entry->count;
        }
        valid_size = total_size - (valid_size - last_node_size);

        return std::make_pair(total_size, valid_size);
    }

    UInt64 getRightMargin() override
    {
        struct rb_node * node = rb_tree_last(&rb_tree->root);
        if (node == nullptr)
        {
            return end;
        }

        auto * entry = node_to_entry(node);
        return entry->start;
    }

protected:
    RBTreeSpaceMap(UInt64 start, UInt64 end)
        : SpaceMap(start, end, SMAP64_RBTREE)
    {
    }

    void freeSmap();

    void smapStats() override;

    bool isMarkUnused(UInt64 block, size_t num) override;

    bool markUsedImpl(UInt64 block, size_t num) override;

    bool markFreeImpl(UInt64 block, size_t num) override;

private:
    struct rb_private * rb_tree;
    UInt64 biggest_range = 0;
    UInt64 biggest_cap = 0;
};

using RBTreeSpaceMapPtr = std::shared_ptr<RBTreeSpaceMap>;

} // namespace PS::V3
} // namespace DB
