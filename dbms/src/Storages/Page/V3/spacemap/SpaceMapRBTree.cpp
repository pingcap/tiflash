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
// Its local debug info, So don't us LOG
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
    struct rb_node *parent = NULL, **n = &root->rb_node;
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

int RBTreeSpaceMap::newSmap()
{
    rb_tree = (struct rb_private *)calloc(1, sizeof(struct rb_private));
    if (rb_tree == NULL)
    {
        return -1;
    }

    rb_tree->root = {
        NULL,
    };
    rb_tree->read_index = NULL;
    rb_tree->read_index_next = NULL;
    rb_tree->write_index = NULL;

    if (rb_insert_entry(start, end, rb_tree) != 0)
    {
        LOG_ERROR(log, "Erorr happend, when mark all range to free.  [start=" << start << "] , [end = " << end << "]");
        free(rb_tree);
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

void RBTreeSpaceMap::freeSmap()
{
    rb_free_tree(&rb_tree->root);
    free(rb_tree);
}

void RBTreeSpaceMap::clearSmap()
{
    if (rb_tree == nullptr)
    {
        LOG_ERROR(log, "SpaceMap have not been inited.");
        return;
    }

    rb_free_tree(&rb_tree->root);
    rb_tree->read_index = NULL;
    rb_tree->read_index_next = NULL;
    rb_tree->write_index = NULL;
    rb_tree_debug(&rb_tree->root, __func__);
}

void RBTreeSpaceMap::smapStats()
{
    struct rb_node * node = NULL;
    struct smap_rb_entry * entry;
    UInt64 count = 0;
    UInt64 max_size = 0;
    UInt64 min_size = ULONG_MAX;

    if (rb_tree->root.rb_node == nullptr)
    {
        LOG_ERROR(log, "Tree have not been inited.");
        return;
    }

    LOG_DEBUG(log, "entry status :");
    for (node = rb_tree_first(&rb_tree->root); node != NULL; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        LOG_DEBUG(log, "  range : " << count << " start:" << entry->start << " size : " << entry->count);
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

int RBTreeSpaceMap::testSmapRange(UInt64 _start,
                                  size_t len)
{
    struct rb_node *parent = NULL, **n;
    struct rb_node *node, *next;
    struct smap_rb_entry * entry;
    int retval = 0;

    n = &rb_tree->root.rb_node;
    _start -= start;

    if (len == 0 || rb_tree->root.rb_node == NULL)
    {
        return -1;
    }


    while (*n)
    {
        parent = *n;
        entry = node_to_entry(parent);
        if (_start < entry->start)
        {
            n = &(*n)->node_left;
        }
        else if (_start >= (entry->start + entry->count))
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

        if ((entry->start + entry->count) <= _start)
            continue;

        /* No more merging */
        if ((_start + len) <= entry->start)
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

void RBTreeSpaceMap::searchRange(size_t size, UInt64 * ret, UInt64 * max_cap)
{
    struct rb_node * node = NULL;
    struct smap_rb_entry * entry;

    UInt64 _biggest_cap = 0;
    UInt64 _biggest_range = 0;
    for (node = rb_tree_first(&rb_tree->root); node != NULL; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        if (entry->count >= size)
        {
            break;
        }
        else
        {
            if (entry->count > _biggest_cap)
            {
                _biggest_cap = entry->count;
                _biggest_range = entry->start;
            }
        }
    }

    // not place found.
    if (!node)
    {
        LOG_ERROR(log, "Not sure why can't found any place to insert.[old biggest_range= " << biggest_range << "] [old biggest_cap=" << biggest_cap << "] [new biggest_range=" << _biggest_range << "] [new biggest_cap=" << _biggest_cap << "]");
        biggest_range = _biggest_range;
        biggest_cap = _biggest_cap;

        *ret = UINT64_MAX;
        *max_cap = 0;
        return;
    }

    // Update return start
    *ret = entry->start;

    if (entry->count == size)
    {
        // It is champion, need update
        if (entry->start == biggest_range)
        {
            struct rb_node * old_node = node;
            node = rb_tree_next(node);
            rb_node_remove(old_node, &rb_tree->root);
            rb_free_entry(rb_tree, entry);
            goto go_on_update_biggest;
        }
        else // It not champion, just return
        {
            rb_node_remove(node, &rb_tree->root);
            rb_free_entry(rb_tree, entry);
            *max_cap = biggest_cap;
            return;
        }
    }
    else // must be entry->count > size
    {
        // Resize this node, no need update
        entry->start += size;
        entry->count -= size;

        // It is champion, need update
        if (entry->start - size == biggest_range)
        {
            if (entry->count > _biggest_cap)
            {
                _biggest_cap = entry->count;
                _biggest_range = entry->start;
            }
            node = rb_tree_next(node);
            goto go_on_update_biggest;
        }
        else // It not champion, just return
        {
            *max_cap = biggest_cap;
            return;
        }
    }

go_on_update_biggest:

    for (; node != NULL; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        if (entry->count > _biggest_cap)
        {
            _biggest_cap = entry->count;
            _biggest_range = entry->start;
        }
    }
    biggest_range = _biggest_range;
    biggest_cap = _biggest_cap;
    *max_cap = biggest_cap;
}

int RBTreeSpaceMap::markSmapRange(UInt64 block, size_t size)
{
    int rc;

    block -= start;

    rc = rb_remove_entry(block, size, rb_tree);
    rb_tree_debug(&rb_tree->root, __func__);
    return rc;
}

int RBTreeSpaceMap::unmarkSmapRange(UInt64 block, size_t size)
{
    int rc;

    block -= start;

    rc = rb_insert_entry(block, size, rb_tree);
    rb_tree_debug(&rb_tree->root, __func__);
    return rc;
}


} // namespace DB::PS::V3