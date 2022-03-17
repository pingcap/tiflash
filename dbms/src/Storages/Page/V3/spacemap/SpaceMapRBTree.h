#pragma once
#include <Storages/Page/V3/spacemap/RBTree.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace PS::V3
{
struct RbPrivate;

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

    UInt64 updateAccurateMaxCapacity() override;

    std::pair<UInt64, UInt64> getSizes() const override;

    UInt64 getRightMargin() override;

protected:
    RBTreeSpaceMap(UInt64 start, UInt64 end)
        : SpaceMap(start, end, SMAP64_RBTREE)
    {
    }

    void freeSmap();

    void smapStats() override;

    bool isMarkUnused(UInt64 offset, size_t length) override;

    bool markUsedImpl(UInt64 offset, size_t length) override;

    bool markFreeImpl(UInt64 offset, size_t length) override;

private:
    struct RbPrivate * rb_tree;
    UInt64 biggest_range = 0;
    UInt64 biggest_cap = 0;
};

using RBTreeSpaceMapPtr = std::shared_ptr<RBTreeSpaceMap>;

} // namespace PS::V3
} // namespace DB
