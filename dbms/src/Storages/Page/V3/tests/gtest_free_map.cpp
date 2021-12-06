#include <Storages/Page/V3/spacemap/RBTree.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/Page/V3/spacemap/SpaceMapRBTree.h>
#include <Storages/Page/V3/spacemap/SpaceMapSTDMap.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <map>

namespace DB::PS::V3::tests
{
struct Range
{
    size_t start;
    size_t end;
};

bool check_nodes(struct rb_root * root, Range * ranges, size_t size)
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

bool check_nodes(std::map<UInt64, UInt64> & map, Range * ranges, size_t size)
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
    ASSERT_TRUE(smap->newSmap());

    struct rb_private * bp = (struct rb_private *)smap->rb_tree;

    Range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));

    ASSERT_TRUE(smap->markUsed(50, 1));
    ASSERT_FALSE(smap->markUsed(50, 1));

    ASSERT_FALSE(smap->isMarkUsed(50, 1));
    ASSERT_TRUE(smap->isMarkUsed(51, 1));

    Range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 51,
                        .end = 100}};

    ASSERT_TRUE(check_nodes(&bp->root, ranges1, 2));

    smap->markFree(50, 1);
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));
    ASSERT_TRUE(smap->isMarkUsed(50, 1));
}

TEST(SpaceMapTest, MarkmarkFreeRBTree)
{
    RBTreeSpaceMapPtr smap = std::make_shared<RBTreeSpaceMap>(0, 100);
    ASSERT_TRUE(smap->newSmap());

    struct rb_private * bp = (struct rb_private *)smap->rb_tree;

    Range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));
    ASSERT_TRUE(smap->isMarkUsed(1, 99));
    bool will_throw_exception = false;
    try
    {
        smap->isMarkUsed(0, 1000);
    }
    catch (DB::Exception e)
    {
        will_throw_exception = true;
    }
    ASSERT_TRUE(will_throw_exception);

    ASSERT_TRUE(smap->markUsed(50, 10));
    ASSERT_FALSE(smap->markUsed(50, 10));
    ASSERT_FALSE(smap->markUsed(50, 9));
    ASSERT_FALSE(smap->markUsed(55, 5));
    Range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges1, 2));
    ASSERT_FALSE(smap->isMarkUsed(51, 5));

    smap->markFree(50, 5);
    Range ranges2[] = {{.start = 0,
                        .end = 55},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges2, 2));
    smap->markFree(55, 5);
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 2));
}

TEST(SpaceMapTest, MarkmarkFreeSTDMap)
{
    STDMapSpaceMapPtr smap = std::make_shared<STDMapSpaceMap>(0, 100);
    ASSERT_TRUE(smap->newSmap());

    Range ranges[] = {{.start = 0,
                       .end = 100}};

    ASSERT_TRUE(check_nodes(smap->free_map, ranges, 1));
    ASSERT_TRUE(smap->isMarkUsed(1, 99));
    bool will_throw_exception = false;
    try
    {
        smap->isMarkUsed(0, 1000);
    }
    catch (DB::Exception e)
    {
        will_throw_exception = true;
    }
    ASSERT_TRUE(will_throw_exception);

    ASSERT_TRUE(smap->markUsed(50, 20));
    ASSERT_FALSE(smap->markUsed(50, 1));
    ASSERT_FALSE(smap->markUsed(50, 20));
    ASSERT_FALSE(smap->markUsed(55, 15));
    Range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 70,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->free_map, ranges1, 2));
    ASSERT_FALSE(smap->isMarkUsed(51, 5));

    smap->markFree(50, 5);
    Range ranges2[] = {{.start = 0,
                        .end = 55},
                       {.start = 70,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->free_map, ranges2, 2));

    smap->markFree(60, 5);
    Range ranges3[] = {{.start = 0,
                        .end = 55},
                       {.start = 60,
                        .end = 65},
                       {.start = 70,
                        .end = 100}};

    ASSERT_TRUE(check_nodes(smap->free_map, ranges3, 3));

    smap->markFree(65, 5);
    Range ranges4[] = {{.start = 0,
                        .end = 55},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->free_map, ranges4, 2));


    smap->markFree(55, 5);
    ASSERT_TRUE(check_nodes(smap->free_map, ranges, 2));
}

TEST(SpaceMapTest, TestMarginsRBTree)
{
    RBTreeSpaceMapPtr smap = std::make_shared<RBTreeSpaceMap>(0, 100);
    ASSERT_TRUE(smap->newSmap());

    struct rb_private * bp = (struct rb_private *)smap->rb_tree;

    Range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges, 1));
    ASSERT_TRUE(smap->markUsed(50, 10));

    Range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(&bp->root, ranges1, 2));

    ASSERT_FALSE(smap->isMarkUsed(50, 5));
    ASSERT_TRUE(smap->isMarkUsed(60, 1));
}


TEST(SpaceMapTest, TestMarginsSTDMap)
{
    STDMapSpaceMapPtr smap = std::make_shared<STDMapSpaceMap>(0, 100);
    ASSERT_TRUE(smap->newSmap());

    Range ranges[] = {{.start = 0,
                       .end = 100}};
    ASSERT_TRUE(check_nodes(smap->free_map, ranges, 1));
    ASSERT_TRUE(smap->markUsed(50, 10));

    Range ranges1[] = {{.start = 0,
                        .end = 50},
                       {.start = 60,
                        .end = 100}};
    ASSERT_TRUE(check_nodes(smap->free_map, ranges1, 2));

    ASSERT_FALSE(smap->isMarkUsed(50, 1));
    ASSERT_TRUE(smap->isMarkUsed(60, 1));
}

} // namespace DB::PS::V3::tests