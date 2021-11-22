#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#ifdef __cplusplus
extern "C" {
#endif
#include <Storages/Page/V3/spacemap/rb_tree.cpp>
#include <Storages/Page/V3/spacemap/space_map.cpp>
#include <Storages/Page/V3/spacemap/space_map_rbtree.cpp>
#ifdef __cplusplus
} // extern "C"
#endif

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
        if (i >= size && ranges[i].start != ext->start && ranges[i].end != ext->start + ext->count)
        {
            return false;
        }
        i++;
    }

    return true;
}

TEST(space_map_test, InitAndDestory)
{
    struct spacemap * smap;

    smap = (struct spacemap *)calloc(1, sizeof(struct spacemap));
    ASSERT_TRUE(smap);
    ASSERT_EQ(init_space_map(smap, SMAP64_RBTREE, 0, 100, 101), 0);
    printf_smap(smap);
    destory_space_map(smap);
}

TEST(space_map_test, MarkUnmarkBit)
{
    struct spacemap * smap;

    smap = (struct spacemap *)calloc(1, sizeof(struct spacemap));
    ASSERT_TRUE(smap);
    ASSERT_EQ(init_space_map(smap, SMAP64_RBTREE, 0, 100, 101), 0);

    struct rb_private * bp = (struct rb_private *)smap->_private;

    range ranges[] = {{.start = 0,
                       .end = 100}};
    check_nodes(&bp->root, ranges, 1);

    ASSERT_TRUE(mark_smap(smap, 50));
    ASSERT_EQ(test_smap(smap, 50), 0);
    ASSERT_EQ(test_smap(smap, 51), 1);

    range ranges1[] = {{.start = 0,
                        .end = 49},
                       {.start = 50,
                        .end = 100}};
    check_nodes(&bp->root, ranges1, 2);

    unmark_smap(smap, 50);
    check_nodes(&bp->root, ranges, 1);
    ASSERT_EQ(test_smap(smap, 50), 1);

    destory_space_map(smap);
}

TEST(space_map_test, MarkUnmarkRange)
{
    struct spacemap * smap;

    smap = (struct spacemap *)calloc(1, sizeof(struct spacemap));
    ASSERT_TRUE(smap);
    ASSERT_EQ(init_space_map(smap, SMAP64_RBTREE, 0, 100, 101), 0);

    struct rb_private * bp = (struct rb_private *)smap->_private;

    range ranges[] = {{.start = 0,
                       .end = 100}};
    check_nodes(&bp->root, ranges, 1);
    ASSERT_EQ(test_smap_range(smap, 1, 99), 1);
    ASSERT_EQ(test_smap_range(smap, 0, 1000), -1);

    ASSERT_TRUE(mark_smap_range(smap, 50, 10));

    range ranges1[] = {{.start = 0,
                        .end = 49},
                       {.start = 60,
                        .end = 100}};
    check_nodes(&bp->root, ranges1, 2);
    ASSERT_EQ(test_smap_range(smap, 51, 5), 0);

    unmark_smap_range(smap, 50, 5);
    range ranges2[] = {{.start = 0,
                        .end = 55},
                       {.start = 60,
                        .end = 100}};
    check_nodes(&bp->root, ranges2, 2);
    unmark_smap_range(smap, 55, 5);
    check_nodes(&bp->root, ranges, 2);

    destory_space_map(smap);
}

} // namespace DB::PS::V3::tests