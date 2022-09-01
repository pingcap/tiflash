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
#include <Storages/DeltaMerge/ReadThread/CircularScanList.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <memory>

namespace DB::DM::tests
{
class Node
{
public:
    explicit Node(uint64_t id_)
        : id(id_)
        , table_id(1)
        , v(true)
    {}

    bool valid() const
    {
        return v;
    }
    uint64_t poolId() const
    {
        return id;
    }
    int64_t tableId() const
    {
        return table_id;
    }
    void setInvalid()
    {
        v = false;
    }

private:
    uint64_t id;
    int64_t table_id;
    bool v;
};

TEST(CircularScanListTest, Normal)
{
    CircularScanList<Node> lst;

    {
        ASSERT_EQ(lst.next(), nullptr);
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 0);
        ASSERT_EQ(invalid, 0);
        ASSERT_EQ(lst.get(1), nullptr);
    }

    std::unordered_map<uint64_t, std::shared_ptr<Node>> nodes;
    for (uint64_t i = 0; i < 10; i++)
    {
        auto p = std::make_shared<Node>(i);
        lst.add(p);
        nodes.emplace(i, p);
    }

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 10);
        ASSERT_EQ(invalid, 0);
    }

    for (uint64_t i = 0; i < 20; i++)
    {
        auto sp = lst.next();
        ASSERT_EQ(sp->poolId(), i % 10);
    }

    lst.get(1)->setInvalid();
    lst.get(3)->setInvalid();
    lst.get(5)->setInvalid();

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 7);
        ASSERT_EQ(invalid, 3);
    }

    // Invalid node still can be scanned if it is holded by other components.
    for (uint64_t i = 0; i < 20; i++)
    {
        auto sp = lst.next();
        ASSERT_EQ(sp->poolId(), i % 10);
    }

    nodes.erase(1);
    nodes.erase(3);
    nodes.erase(5);

    const std::vector<uint64_t> valid_ids = {0, 2, 4, 6, 7, 8, 9};
    for (uint64_t i = 0; i < 20; i++)
    {
        auto sp = lst.next();
        ASSERT_EQ(sp->poolId(), valid_ids[i % valid_ids.size()]);
    }

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 7);
        ASSERT_EQ(invalid, 0);
    }

    for (uint64_t id : valid_ids)
    {
        lst.get(id)->setInvalid();
        nodes.erase(id);
    }

    {
        auto [valid, invalid] = lst.count(0);
        ASSERT_EQ(valid, 0);
        ASSERT_EQ(invalid, 7);
    }

    ASSERT_EQ(lst.next(), nullptr);
}

TEST(CircularScanListTest, Valid)
{
    CircularScanList<Node> l;
    auto p1 = std::make_shared<Node>(1);
    l.add(p1);

    ASSERT_EQ(l.next()->poolId(), 1);
    ASSERT_EQ(l.next()->poolId(), 1);

    l.next()->setInvalid();
    p1.reset();

    ASSERT_EQ(l.next(), nullptr);
    ASSERT_EQ(l.next(), nullptr);
    auto p2 = std::make_shared<Node>(2);
    l.add(p2);

    ASSERT_EQ(l.next()->poolId(), 2);
    ASSERT_EQ(l.next()->poolId(), 2);
}

TEST(CircularScanListTest, ScheduleInvalid)
{
    CircularScanList<Node> l;

    // Add tasks.
    auto n1 = std::make_shared<Node>(1);
    l.add(n1);
    auto n2 = std::make_shared<Node>(2);
    l.add(n2);
    auto n3 = std::make_shared<Node>(3);
    l.add(n3);

    // Some tasks hold the shared_ptr.
    //auto n1 = l.next();
    //auto n2 = l.next();
    //auto n3 = l.next();

    {
        auto [valid, invalid] = l.count(0);
        ASSERT_EQ(valid, 3);
        ASSERT_EQ(invalid, 0);
    }
    // Make task invalid.
    n1->setInvalid();
    n2->setInvalid();
    n3->setInvalid();

    {
        auto [valid, invalid] = l.count(0);
        ASSERT_EQ(valid, 0);
        ASSERT_EQ(invalid, 3);
    }

    {
        // Tasks can be scheduled.
        auto n1_1 = l.next();
        ASSERT_NE(n1_1, nullptr);
        ASSERT_EQ(n1_1->poolId(), 1);
        ASSERT_FALSE(n1_1->valid());

        auto n2_1 = l.next();
        ASSERT_NE(n2_1, nullptr);
        ASSERT_EQ(n2_1->poolId(), 2);
        ASSERT_FALSE(n2_1->valid());

        auto n3_1 = l.next();
        ASSERT_NE(n3_1, nullptr);
        ASSERT_EQ(n3_1->poolId(), 3);
        ASSERT_FALSE(n3_1->valid());
    }

    // Reset tasks
    {
        n1.reset();
        n2.reset();
        n3.reset();
    }

    {
        auto [valid, invalid] = l.count(0);
        ASSERT_EQ(valid, 0);
        ASSERT_EQ(invalid, 3);
    }

    // Tasks no need to be scheduled since no task hold the shared_ptr.
    {
        auto n1_1 = l.next();
        ASSERT_EQ(n1_1, nullptr);

        auto n2_1 = l.next();
        ASSERT_EQ(n2_1, nullptr);

        auto n3_1 = l.next();
        ASSERT_EQ(n3_1, nullptr);
    }

    {
        auto [valid, invalid] = l.count(0);
        ASSERT_EQ(valid, 0);
        ASSERT_EQ(invalid, 0);
    }
}

} // namespace DB::DM::tests