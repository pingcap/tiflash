// Copyright 2023 PingCAP, Inc.
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

#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace DM
{
namespace tests
{
class DeltaIndexManagerTest : public ::testing::Test
{
public:
    DeltaIndexManagerTest()
        : one_node_size(DefaultDeltaTree().getBytes())
    {}

protected:
    size_t one_node_size;
};

DeltaIndexPtr genDeltaIndex()
{
    auto delta_tree = std::make_shared<DefaultDeltaTree>();
    delta_tree->addInsert(1, 0);
    return std::make_shared<DeltaIndex>(delta_tree, 1, 0);
}


TEST_F(DeltaIndexManagerTest, LRU)
try
{
    DeltaIndexManager manager(one_node_size * 100);

    std::vector<DeltaIndexPtr> indies;
    for (int i = 0; i < 200; ++i)
    {
        indies.push_back(genDeltaIndex());
    }
    for (int i = 0; i < 100; ++i)
    {
        ASSERT_EQ(manager.currentSize(), one_node_size * i);
        manager.refreshRef(indies[i]);
    }

    ASSERT_EQ(manager.currentSize(), one_node_size * (100));

    for (int i = 0; i < 100; ++i)
    {
        ASSERT_EQ(manager.getRef(indies[i]->getId()), indies[i]);
    }


    for (int i = 100; i < 200; ++i)
    {
        ASSERT_EQ(manager.currentSize(), one_node_size * 100);
        manager.refreshRef(indies[i]);
    }
    for (int i = 0; i < 100; ++i)
    {
        ASSERT_EQ(manager.getRef(indies[i]->getId()), DeltaIndexPtr());
        ASSERT_EQ(indies[i]->getPlacedStatus(), std::make_pair((size_t)0, (size_t)0));
    }
    for (int i = 100; i < 200; ++i)
    {
        ASSERT_EQ(manager.getRef(indies[i]->getId()), indies[i]);
    }

    for (int i = 100; i < 150; ++i)
    {
        manager.deleteRef(indies[i]);
    }
    ASSERT_EQ(manager.currentSize(), one_node_size * (50));

    for (int i = 100; i < 150; ++i)
    {
        ASSERT_EQ(manager.getRef(indies[i]->getId()), DeltaIndexPtr());
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
