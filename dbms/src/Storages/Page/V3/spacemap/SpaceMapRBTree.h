// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    std::tuple<UInt64, UInt64, bool> searchInsertOffset(size_t size) override;

    UInt64 updateAccurateMaxCapacity() override;

    std::pair<UInt64, UInt64> getSizes() const override;

    UInt64 getUsedBoundary() override;

protected:
    RBTreeSpaceMap(UInt64 start, UInt64 end)
        : SpaceMap(start, end, SMAP64_RBTREE)
    {
    }

    void freeSmap();

    String toDebugString() override;

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
