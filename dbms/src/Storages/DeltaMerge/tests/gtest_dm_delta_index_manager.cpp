#define private public
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#undef private

#include <Storages/DeltaMerge/DeltaTree.h>
#include <gtest/gtest.h>
#include <test_utils/TiflashTestBasic.h>

namespace DB
{
namespace DM
{
namespace tests
{

class DeltaIndexManager_test : public ::testing::Test
{
public:
    DeltaIndexManager_test()
        : parent_path(DB::tests::TiFlashTestEnv::getTemporaryPath() + "/dm_delta_index_manager"),
          one_node_size(DefaultDeltaTree().getBytes())
    {
    }

protected:
    String parent_path;
    size_t one_node_size;
};

DeltaIndexPtr genDeltaIndex()
{
    auto delta_tree = std::make_shared<DefaultDeltaTree>();
    delta_tree->addInsert(1, 0);
    return std::make_shared<DeltaIndex>(delta_tree, 1, 0);
}


TEST_F(DeltaIndexManager_test, LRU)
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
        ASSERT_EQ(manager.current_size, one_node_size * i);
        manager.refresh(indies[i]);
    }

    ASSERT_EQ(manager.current_size, one_node_size * (100));

    for (int i = 0; i < 100; ++i)
    {
        ASSERT_EQ(manager.get(indies[i]->getId()), indies[i]);
    }


    for (int i = 100; i < 200; ++i)
    {
        ASSERT_EQ(manager.current_size, one_node_size * 100);
        manager.refresh(indies[i]);
    }
    for (int i = 0; i < 100; ++i)
    {
        ASSERT_EQ(manager.get(indies[i]->getId()), DeltaIndexPtr());
        ASSERT_EQ(indies[i]->getPlacedStatus(), std::make_pair((size_t)0, (size_t)0));
    }
    for (int i = 100; i < 200; ++i)
    {
        ASSERT_EQ(manager.get(indies[i]->getId()), indies[i]);
    }

    for (int i = 100; i < 150; ++i)
    {
        manager.remove(indies[i]);
    }
    ASSERT_EQ(manager.current_size, one_node_size * (50));

    for (int i = 100; i < 150; ++i)
    {
        ASSERT_EQ(manager.get(indies[i]->getId()), DeltaIndexPtr());
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB