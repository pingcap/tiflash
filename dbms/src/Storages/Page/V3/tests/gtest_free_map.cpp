#include <Storages/Page/V3/spacemap/RBTree.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/Page/V3/spacemap/SpaceMapRBTree.h>
#include <Storages/Page/V3/spacemap/SpaceMapSTDMap.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <map>

namespace DB::PS::V3::tests
{
struct range
{
    size_t start;
    size_t end;
};

bool check_nodes(struct rb_root * root, range * ranges, size_t size)
{
    struct rb_node * node = NULL;
    struct smap_rb_entry * ext;

    assert(size != 0);

    size_t i = 0;
    for (node = rb_tree_first(root); node != NULL; node = rb_tree_next(node))
    {
        ext = node_to_entry(node);
        if (i >= size || ranges[i].start != ext->start || ranges[i].end != ext->start + ext->count)
        {
            return false;
        }
        i++;
    }

    return true;
}

bool check_nodes(std::map<UInt64, UInt64> & map, range * ranges, size_t size)
{
    assert(size != 0);

    size_t i = 0;
    for (auto it = map.begin(); it != map.end(); it++)
    {
        if (i >= size || ranges[i].start != it->first || ranges[i].end != it->first + it->second)
        {
            return false;
        }
        i++;
    }

    return true;
}


TEST(SpaceMapTest, InitAndDestory)
{
    SpaceMapPtr smap = SpaceMap::createSpaceMap(SpaceMap::SpaceMapType::SMAP64_RBTREE, 0, 100);

    smap->logStats();
}


TEST(SpaceMapTest, MarkUnmarkBitRBTree)
{
    RBTreeSpaceMapPtr smap = std::make_shared<RBTreeSpaceMap>(0, 100);
    ASSERT_EQ(smap->newSmap(), 0);

    struct rb_private * bp = (struct rb_private *)smap->rb_tree;

    range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));

    ASSERT_TRUE(smap->markRange(50, 1));
    ASSERT_EQ(smap->testRange(50, 1), 0);
    ASSERT_EQ(smap->testRange(51, 1), 1);

    range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 51,
                        .end = 100}};

    ASSERT_TRUE(check_nodes(&bp->root, ranges1, 2));

    smap->unmarkRange(50, 1);
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));
    ASSERT_EQ(smap->testRange(50, 1), 1);
}

TEST(SpaceMapTest, MarkUnmarkRangeRBTree)
{
    RBTreeSpaceMapPtr smap = std::make_shared<RBTreeSpaceMap>(0, 100);
    ASSERT_EQ(smap->newSmap(), 0);

    struct rb_private * bp = (struct rb_private *)smap->rb_tree;

    range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));
    ASSERT_EQ(smap->testRange(1, 99), 1);
    ASSERT_EQ(smap->testRange(0, 1000), -1);

    ASSERT_TRUE(smap->markRange(50, 10));

    range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges1, 2));
    ASSERT_EQ(smap->testRange(51, 5), 0);

    smap->unmarkRange(50, 5);
    range ranges2[] = {{.start = 0,
                        .end = 55},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges2, 2));
    smap->unmarkRange(55, 5);
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 2));
}

TEST(SpaceMapTest, MarkUnmarkRangeSTDMap)
{
    STDMapSpaceMapPtr smap = std::make_shared<STDMapSpaceMap>(0, 100);
    ASSERT_EQ(smap->newSmap(), 0);

    range ranges[] = {{.start = 0,
                       .end = 100}};

    ASSERT_TRUE(check_nodes(smap->map, ranges, 1));
    ASSERT_EQ(smap->testRange(1, 99), 1);
    ASSERT_EQ(smap->testRange(0, 1000), -1);

    ASSERT_TRUE(smap->markRange(50, 20));

    range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 70,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->map, ranges1, 2));
    ASSERT_EQ(smap->testRange(51, 5), 0);

    smap->unmarkRange(50, 5);
    range ranges2[] = {{.start = 0,
                        .end = 55},
                       {.start = 70,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->map, ranges2, 2));

    smap->unmarkRange(60, 5);
    range ranges3[] = {{.start = 0,
                        .end = 55},
                       {.start = 60,
                        .end = 65},
                       {.start = 70,
                        .end = 100}};

    ASSERT_TRUE(check_nodes(smap->map, ranges3, 3));

    smap->unmarkRange(65, 5);
    range ranges4[] = {{.start = 0,
                        .end = 55},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->map, ranges4, 2));


    smap->unmarkRange(55, 5);
    ASSERT_TRUE(check_nodes(smap->map, ranges, 2));
}

TEST(SpaceMapTest, TestMarginsRBTree)
{
    RBTreeSpaceMapPtr smap = std::make_shared<RBTreeSpaceMap>(0, 100);
    ASSERT_EQ(smap->newSmap(), 0);

    struct rb_private * bp = (struct rb_private *)smap->rb_tree;

    range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));
    ASSERT_TRUE(smap->markRange(50, 10));

    range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges1, 2));

    ASSERT_EQ(smap->testRange(50, 5), 0);
    ASSERT_EQ(smap->testRange(60, 1), 1);
}


TEST(SpaceMapTest, TestMarginsSTDMap)
{
    STDMapSpaceMapPtr smap = std::make_shared<STDMapSpaceMap>(0, 100);
    ASSERT_EQ(smap->newSmap(), 0);

    range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(smap->map, ranges, 1));
    ASSERT_TRUE(smap->markRange(50, 10));

    range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->map, ranges1, 2));

    ASSERT_EQ(smap->testRange(50, 1), 0);
    ASSERT_EQ(smap->testRange(60, 1), 1);
}


} // namespace DB::PS::V3::tests