#include "SpaceMapRBTree.h"

namespace DB::PS::V3 
{

static int rb_insert_entry(UInt64 start, UInt64 count, struct rb_private * private_data);
static int rb_remove_entry(UInt64 start, UInt64 count, struct rb_private * private_data);

static inline void rb_link_node(struct rb_node * node,
                                struct rb_node * parent,
                                struct rb_node ** rb_link)
{
    node->parent = (uintptr_t)parent;
    node->node_left = NULL;
    node->node_right = NULL;

    *rb_link = node;
}

#define ENABLE_DEBUG_IN_RB_TREE 0

#if !defined(NDEBUG) && !defined(DBMS_PUBLIC_GTEST) && ENABLE_DEBUG_IN_RB_TREE
static void rb_tree_debug(struct rb_root * root, const char * method_call)
{
    struct rb_node * node = NULL;
    struct smap_rb_entry * entry;

    node = rb_tree_first(root);
    printf("call in %s", method_call);
    for (node = rb_tree_first(root); node != NULL; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        printf(" range - (%llu -> %llu)\n", entry->start, entry->start + entry->count);
    }
}

#else
#define rb_tree_debug(root, function) \
    do                                \
    {                                 \
    } while (0)
#endif


static void rb_get_new_entry(struct smap_rb_entry ** entry, UInt64 start, UInt64 count)
{
    struct smap_rb_entry * new_entry;

    new_entry = (struct smap_rb_entry *)calloc(1, sizeof(struct smap_rb_entry));
    if (new_entry == NULL)
    {
        return;
    }

    new_entry->start = start;
    new_entry->count = count;
    *entry = new_entry;
}

inline static void rb_free_entry(struct rb_private * private_data, struct smap_rb_entry * entry)
{
    /**
     * reset all index
     */
    if (private_data->write_index == entry)
    {
        private_data->write_index = NULL;
    }

    if (private_data->read_index == entry)
    {
        private_data->read_index = NULL;
    }

    if (private_data->read_index_next == entry)
    {
        private_data->read_index_next = NULL;
    }

    free(entry);
}

static int rb_remove_entry(UInt64 start, UInt64 count, struct rb_private * private_data)
{
    struct rb_root * root = &private_data->root;
    struct rb_node * parent = NULL, **n = &root->rb_node;
    struct rb_node * node;
    struct smap_rb_entry * entry;
    UInt64 new_start, new_count;
    int retval = 0;

    // Root node have not been init
    if (private_data->root.rb_node == NULL)
    {
        return 0;
    }

    while (*n)
    {
        parent = *n;
        entry = node_to_entry(parent);
        if (start < entry->start)
        {
            n = &(*n)->node_left;
            continue;
        }
        else if (start >= (entry->start + entry->count))
        {
            n = &(*n)->node_right;
            continue;
        }

        if ((start > entry->start) && (start + count) < (entry->start + entry->count))
        {
            // Split entry
            new_start = start + count;
            new_count = (entry->start + entry->count) - new_start;

            entry->count = start - entry->start;

            rb_insert_entry(new_start, new_count, private_data);
            return 1;
        }

        if ((start + count) >= (entry->start + entry->count))
        {
            entry->count = start - entry->start;
            retval = 1;
        }

        if (0 == entry->count)
        {
            parent = rb_tree_next(&entry->node);
            rb_node_remove(&entry->node, root);
            rb_free_entry(private_data, entry);
            break;
        }

        if (start == entry->start)
        {
            entry->start += count;
            entry->count -= count;
            return 1;
        }
    }

    // Check the right node
    for (; parent != NULL; parent = node)
    {
        node = rb_tree_next(parent);
        entry = node_to_entry(parent);
        if ((entry->start + entry->count) <= start)
            continue;

        if ((start + count) < entry->start)
            break;

        // Merge the nearby node
        //  TBD : If there are a small range inside two range, Then it should be ignored
        if ((start + count) >= (entry->start + entry->count))
        {
            rb_node_remove(parent, root);
            rb_free_entry(private_data, entry);
            retval = 1;
            continue;
        }
        else
        {
            entry->count -= ((start + count) - entry->start);
            entry->start = start + count;
            retval = 1;
            break;
        }
    }

    return retval;
}

static int rb_alloc_private_data(SpaceMap * smap)
{
    struct rb_private * private_data;

    // alloc root node
    private_data = (struct rb_private *)calloc(1, sizeof(struct rb_private));
    if (private_data == NULL)
    {
        return -1;
    }

    private_data->root = {NULL};
    private_data->read_index = NULL;
    private_data->read_index_next = NULL;
    private_data->write_index = NULL;

    smap->_private = (void *)private_data;
    return 0;
}

int RBTreeSpaceMapOps::newSmap(SpaceMap * smap)
{
    int ret;
    struct rb_private * private_data;

    ret = rb_alloc_private_data(smap);
    if (ret)
    {
        return ret;
    }

    private_data = (struct rb_private *)smap->_private;

    ret = rb_insert_entry(smap->start, smap->end, private_data);
    if (ret != 0)
    {
        printf("Erorr happend, when mark all range to free.\n");
        free(private_data);
        return -1;
    }

    return 0;
}

static void rb_free_tree(struct rb_root * root)
{
    struct smap_rb_entry * entry;
    struct rb_node *node, *next;

    for (node = rb_tree_first(root); node; node = next)
    {
        next = rb_tree_next(node);
        entry = node_to_entry(node);
        rb_node_remove(node, root);
        free(entry);
    }
}

void RBTreeSpaceMapOps::freeSmap(SpaceMap * smap)
{
    struct rb_private * private_data;

    private_data = (struct rb_private *)smap->_private;

    rb_free_tree(&private_data->root);
    free(private_data);
}

void RBTreeSpaceMapOps::clearSmap(SpaceMap * smap)
{
    struct rb_private * private_data;

    private_data = (struct rb_private *)smap->_private;

    rb_free_tree(&private_data->root);
    private_data->read_index = NULL;
    private_data->read_index_next = NULL;
    private_data->write_index = NULL;
    rb_tree_debug(&private_data->root, __func__);
}

void RBTreeSpaceMapOps::smapStats(SpaceMap * smap)
{
    struct rb_private * private_data;
    struct rb_node * node = NULL;
    struct smap_rb_entry * entry;
    UInt64 count = 0;
    UInt64 max_size = 0;
    UInt64 min_size = ULONG_MAX;

    private_data = (struct rb_private *)smap->_private;
    printf("entry status :\n");
    for (node = rb_tree_first(&private_data->root); node != NULL; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        printf("range %lld : start=%lld , count=%lld \n", count, entry->start, entry->count);
        count++;
        if (entry->count > max_size)
        {
            max_size = entry->count;
        }
            
        if (entry->count < min_size)
        {
            min_size = entry->count;
        }
    }
}


inline static int rb_test_bit(struct rb_private * private_data, UInt64 bit)
{
    struct smap_rb_entry *read_index, *next_entry = NULL;
    struct rb_node *parent = NULL, *next_node;
    struct rb_node ** n = &private_data->root.rb_node;
    struct smap_rb_entry * entry;

    read_index = private_data->read_index;

    // derect search tree
    if (!read_index)
    {
        goto search_tree;
    }


    if (bit >= read_index->start && bit < read_index->start + read_index->count)
    {
        return 1;
    }

    next_entry = private_data->read_index_next;
    if (!next_entry)
    {
        next_node = rb_tree_next(&read_index->node);
        if (next_node)
        {
            next_entry = node_to_entry(next_node);
        }
        private_data->read_index_next = next_entry;
    }
    if (next_entry)
    {
        if ((bit >= read_index->start + read_index->count) && (bit < next_entry->start))
        {
            return 0;
        }
    }
    private_data->read_index = NULL;
    private_data->read_index_next = NULL;

    read_index = private_data->write_index;
    if (!read_index)
    {
        goto search_tree;
    }

    if (bit >= read_index->start && bit < read_index->start + read_index->count)
    {
        return 1;
    }

search_tree:

    while (*n)
    {
        parent = *n;
        entry = node_to_entry(parent);
        if (bit < entry->start)
        {
            n = &(*n)->node_left;
        }
        else if (bit >= (entry->start + entry->count))
        {
            n = &(*n)->node_right;
        }
        else
        {
            private_data->read_index = entry;
            private_data->read_index_next = NULL;
            return 1;
        }
    }
    return 0;
}

int RBTreeSpaceMapOps::testSmapBit(SpaceMap * smap, UInt64 arg)
{
    struct rb_private * private_data;

    private_data = (struct rb_private *)smap->_private;
    arg -= smap->start;

    return rb_test_bit(private_data, arg);
}


int RBTreeSpaceMapOps::testSmapRange(SpaceMap * smap,
                              UInt64 start,
                              unsigned int len)
{
    struct rb_node *parent = NULL, **n;
    struct rb_node *node, *next;
    struct rb_private * private_data;
    struct smap_rb_entry * entry;
    int retval = 0;

    private_data = (struct rb_private *)smap->_private;
    n = &private_data->root.rb_node;
    start -= smap->start;

    if (len == 0 || private_data->root.rb_node == NULL)
    {
        return -1;
    }


    while (*n)
    {
        parent = *n;
        entry = node_to_entry(parent);
        if (start < entry->start)
        {
            n = &(*n)->node_left;
        }
        else if (start >= (entry->start + entry->count))
        {
            n = &(*n)->node_right;
        }
        else
        {
            // the tree -> entry is not clear
            // so just return
            return 1;
        }
    }

    node = parent;
    while (node)
    {
        next = rb_tree_next(node);
        entry = node_to_entry(node);
        node = next;

        if ((entry->start + entry->count) <= start)
            continue;

        /* No more merging */
        if ((start + len) <= entry->start)
            break;

        retval = 1;
        break;
    }
    return retval;
}

static int rb_insert_entry(UInt64 start, UInt64 count, struct rb_private * private_data)
{
    struct rb_root * root = &private_data->root;
    struct rb_node *parent = NULL, **n = &root->rb_node;
    struct rb_node *new_node, *node, *next;
    struct smap_rb_entry * new_entry;
    struct smap_rb_entry * entry;
    int retval = 0;

    if (count == 0)
    {
        return retval;
    }


    private_data->read_index_next = NULL;
    entry = private_data->write_index;
    if (entry)
    {
        if (start >= entry->start && start <= (entry->start + entry->count))
        {
            goto got_entry;
        }
    }

    while (*n)
    {
        parent = *n;
        entry = node_to_entry(parent);

        if (start < entry->start)
        {
            n = &(*n)->node_left;
        }
        else if (start > (entry->start + entry->count))
        {
            n = &(*n)->node_right;
        }
        else
        {
        got_entry:
            if ((start + count) <= (entry->start + entry->count))
            {
                retval = 1;
                return retval;
            }

            if ((entry->start + entry->count) == start)
            {
                retval = 0;
            }
            else
            {
                retval = 1;
            }

            count += (start - entry->start);
            start = entry->start;
            new_entry = entry;
            new_node = &entry->node;

            goto no_need_insert;
        }
    }

    rb_get_new_entry(&new_entry, start, count);

    new_node = &new_entry->node;
    rb_link_node(new_node, parent, n);
    rb_node_insert(new_node, root);
    private_data->write_index = new_entry;

    node = rb_tree_prev(new_node);
    if (node)
    {
        entry = node_to_entry(node);
        if ((entry->start + entry->count) == start)
        {
            start = entry->start;
            count += entry->count;
            rb_node_remove(node, root);
            rb_free_entry(private_data, entry);
        }
    }

no_need_insert:
    // merge entry to the right
    for (node = rb_tree_next(new_node); node != NULL; node = next)
    {
        next = rb_tree_next(node);
        entry = node_to_entry(node);

        if ((entry->start + entry->count) <= start)
        {
            continue;
        }


        // not match
        // TBD : same as comment in remove
        if ((start + count) < entry->start)
            break;

        if ((start + count) >= (entry->start + entry->count))
        {
            rb_node_remove(node, root);
            rb_free_entry(private_data, entry);
            continue;
        }
        else
        {
            // merge entry
            // TBD : same as comment in remove
            count += ((entry->start + entry->count) - (start + count));
            rb_node_remove(node, root);
            rb_free_entry(private_data, entry);
            break;
        }
    }

    new_entry->start = start;
    new_entry->count = count;

    return retval;
}

int RBTreeSpaceMapOps::markSmapBit(SpaceMap * smap, UInt64 arg)
{
    struct rb_private * private_data;
    int retval;

    private_data = (struct rb_private *)smap->_private;
    arg -= smap->start;

    retval = rb_remove_entry(arg, 1, private_data);
    rb_tree_debug(&private_data->root, __func__);
    return retval;
}

int RBTreeSpaceMapOps::unmarkSmapBit(SpaceMap * smap, UInt64 arg)
{
    struct rb_private * private_data;
    int retval;

    private_data = (struct rb_private *)smap->_private;
    arg -= smap->start;

    retval = rb_insert_entry(arg, 1, private_data);
    rb_tree_debug(&private_data->root, __func__);

    return retval;
}

int RBTreeSpaceMapOps::markSmapRange(SpaceMap * smap, UInt64 arg, unsigned int num)
{
    struct rb_private * private_data;
    int retval;

    private_data = (struct rb_private *)smap->_private;
    arg -= smap->start;

    retval = rb_remove_entry(arg, num, private_data);
    rb_tree_debug(&private_data->root, __func__);
    return retval;
}

int RBTreeSpaceMapOps::unmarkSmapRange(SpaceMap * smap, UInt64 arg, unsigned int num)
{
    struct rb_private * private_data;
    int retval;

    private_data = (struct rb_private *)smap->_private;
    arg -= smap->start;

    retval = rb_insert_entry(arg, num, private_data);
    rb_tree_debug(&private_data->root, __func__);
    return retval;
}

}