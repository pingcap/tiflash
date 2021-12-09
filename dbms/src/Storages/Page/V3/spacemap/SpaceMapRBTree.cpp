#include <Storages/Page/V3/spacemap/SpaceMapRBTree.h>


namespace DB::PS::V3
{
static bool rb_insert_entry(UInt64 start, UInt64 count, struct rb_private * private_data, Poco::Logger * log);
static bool rb_remove_entry(UInt64 start, UInt64 count, struct rb_private * private_data, Poco::Logger * log);

static inline void rb_link_node(struct rb_node * node,
                                struct rb_node * parent,
                                struct rb_node ** rb_link)
{
    node->parent = (uintptr_t)parent;
    node->node_left = nullptr;
    node->node_right = nullptr;

    *rb_link = node;
}

#define ENABLE_DEBUG_IN_RB_TREE 0

#if !defined(NDEBUG) && !defined(DBMS_PUBLIC_GTEST) && ENABLE_DEBUG_IN_RB_TREE
// Its local debug info, So don't us LOG
static void rb_tree_debug(struct rb_root * root, const char * method_call)
{
    struct rb_node * node = nullptr;
    struct smap_rb_entry * entry;

    node = rb_tree_first(root);
    printf("call in %s", method_call);
    for (node = rb_tree_first(root); node != nullptr; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        printf(" Space - (%llu -> %llu)\n", entry->start, entry->start + entry->count);
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
    if (new_entry == nullptr)
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
        private_data->write_index = nullptr;
    }

    if (private_data->read_index == entry)
    {
        private_data->read_index = nullptr;
    }

    if (private_data->read_index_next == entry)
    {
        private_data->read_index_next = nullptr;
    }

    free(entry);
}


static bool rb_insert_entry(UInt64 start, UInt64 count, struct rb_private * private_data, Poco::Logger * log)
{
    struct rb_root * root = &private_data->root;
    struct rb_node *parent = nullptr, **n = &root->rb_node;
    struct rb_node *new_node, *node, *next;
    struct smap_rb_entry * new_entry;
    struct smap_rb_entry * entry;
    bool retval = true;

    if (count == 0)
    {
        return false;
    }

    private_data->read_index_next = nullptr;
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
                return false;
            }

            if ((entry->start + entry->count) == start)
            {
                retval = true;
                if (parent)
                {
                    auto * _node = rb_tree_next(parent);
                    if (_node)
                    {
                        auto * _entry = node_to_entry(_node);
                        if (start + count > _entry->start)
                        {
                            LOG_WARNING(log, "Marked space free failed. [offset=" << start << ", size=" << count << "], next node is [offset=" << _entry->start << ",size=" << _entry->count << "]");
                            return false;
                        }
                    }
                }
            }
            else
            {
                return false;
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

    /**
     * We need check current node is legal before we merge it.
     * If prev/next node exist. Check if they have overlap with the current node.
     * Also, We can’t check while doing the merge. 
     * Because it will cause the original state not to be restored
     */
    node = rb_tree_prev(new_node);
    if (node)
    {
        entry = node_to_entry(node);
        if (entry->start + entry->count > new_entry->start)
        {
            LOG_WARNING(log, "Marked space free failed. [offset=" << new_entry->start << ", size=" << new_entry->count << "], prev node is [offset=" << entry->start << ",size=" << entry->count << "]");
            rb_node_remove(new_node, root);
            rb_free_entry(private_data, new_entry);
            return false;
        }
    }

    node = rb_tree_next(new_node);
    if (node)
    {
        entry = node_to_entry(node);
        if (new_entry->start + new_entry->count > entry->start)
        {
            LOG_WARNING(log, "Marked space free failed. [offset=" << new_entry->start << ", size=" << new_entry->count << "], next node is [offset=" << entry->start << ",size=" << entry->count << "]");
            rb_node_remove(new_node, root);
            rb_free_entry(private_data, new_entry);
            return false;
        }
    }


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
    for (node = rb_tree_next(new_node); node != nullptr; node = next)
    {
        next = rb_tree_next(node);
        entry = node_to_entry(node);

        if ((entry->start + entry->count) <= start)
        {
            continue;
        }

        // not match
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


static bool rb_remove_entry(UInt64 start, UInt64 count, struct rb_private * private_data, Poco::Logger * log)
{
    struct rb_root * root = &private_data->root;
    struct rb_node *parent = nullptr, **n = &root->rb_node;
    struct rb_node * node;
    struct smap_rb_entry * entry;
    UInt64 new_start, new_count;
    bool marked = false;

    // Root node have not been init
    if (private_data->root.rb_node == nullptr)
    {
        assert(false);
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

        /**
         * We got node.
         * entry->start < start < (entry->start + entry->count)
         */

        if ((start + count) > (entry->start + entry->count))
        {
            LOG_WARNING(log, "Marked space used failed. [offset=" << start << ", size=" << count << "] is bigger than space [offset=" << entry->start << ",size=" << entry->count << "]");
            return false;
        }

        if (start < entry->start)
        {
            LOG_WARNING(log, "Marked space used failed. [offset=" << start << ", size=" << count << "] is less than space [offset=" << entry->start << ",size=" << entry->count << "]");
            return false;
        }

        // In the Mid
        if ((start > entry->start) && (start + count) < (entry->start + entry->count))
        {
            // Split entry
            new_start = start + count;
            new_count = (entry->start + entry->count) - new_start;

            entry->count = start - entry->start;

            rb_insert_entry(new_start, new_count, private_data, log);
            return true;
        }

        // Match right
        if ((start + count) == (entry->start + entry->count))
        {
            entry->count = start - entry->start;
            marked = true;
        }

        // Left have no count remian.
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
            return true;
        }
    }

    // Checking the right node
    for (; parent != nullptr; parent = node)
    {
        node = rb_tree_next(parent);
        entry = node_to_entry(parent);
        if ((entry->start + entry->count) <= start)
            continue;

        if ((start + count) < entry->start)
            break;

        if ((start + count) > entry->start)
            return false;

        // Merge the nearby node
        if ((start + count) >= (entry->start + entry->count))
        {
            rb_node_remove(parent, root);
            rb_free_entry(private_data, entry);
            marked = true;
            continue;
        }
        else
        {
            if (((start + count) - entry->start) == 0
                && (start + count == entry->start))
            {
                break;
            }
            entry->count -= ((start + count) - entry->start);
            entry->start = start + count;
            marked = true;
            break;
        }
    }

    return marked;
}

std::shared_ptr<RBTreeSpaceMap> RBTreeSpaceMap::create(UInt64 start, UInt64 end)
{
    auto ptr = std::shared_ptr<RBTreeSpaceMap>(new RBTreeSpaceMap(start, end));

    ptr->rb_tree = static_cast<struct rb_private *>(calloc(1, sizeof(struct rb_private)));
    if (ptr->rb_tree == nullptr)
    {
        return nullptr;
    }

    ptr->rb_tree->root = {
        nullptr,
    };
    ptr->rb_tree->read_index = nullptr;
    ptr->rb_tree->read_index_next = nullptr;
    ptr->rb_tree->write_index = nullptr;

    if (!rb_insert_entry(start, end, ptr->rb_tree, ptr->log))
    {
        LOG_ERROR(ptr->log, "Erorr happend, when mark all space free.  [start=" << start << "] , [end=" << end << "]");
        free(ptr->rb_tree);
        return nullptr;
    }
    return ptr;
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
    if (rb_tree)
    {
        rb_free_tree(&rb_tree->root);
        free(rb_tree);
    }
}

void RBTreeSpaceMap::smapStats()
{
    struct rb_node * node = nullptr;
    struct smap_rb_entry * entry;
    UInt64 count = 0;
    UInt64 max_size = 0;
    UInt64 min_size = ULONG_MAX;

    if (rb_tree->root.rb_node == nullptr)
    {
        LOG_ERROR(log, "Tree have not been inited.");
        return;
    }

    LOG_DEBUG(log, "RB-Tree entries status: ");
    for (node = rb_tree_first(&rb_tree->root); node != nullptr; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        LOG_DEBUG(log, "  Space: " << count << " start:" << entry->start << " size: " << entry->count);
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

bool RBTreeSpaceMap::isMarkUnused(UInt64 _start,
                                  size_t len)
{
    struct rb_node *parent = nullptr, **n;
    struct rb_node *node, *next;
    struct smap_rb_entry * entry;
    bool retval = false;

    n = &rb_tree->root.rb_node;
    _start -= start;

    if (len == 0 || rb_tree->root.rb_node == nullptr)
    {
        assert(0);
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
            return true;
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

        retval = true;
        break;
    }
    return retval;
}

std::pair<UInt64, UInt64> RBTreeSpaceMap::searchInsertOffset(size_t size)
{
    UInt64 offset = UINT64_MAX;
    UInt64 max_cap = 0;
    struct rb_node * node = nullptr;
    struct smap_rb_entry * entry;

    UInt64 _biggest_cap = 0;
    UInt64 _biggest_range = 0;
    for (node = rb_tree_first(&rb_tree->root); node != nullptr; node = rb_tree_next(node))
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

    // No enough space for insert
    if (!node)
    {
        LOG_ERROR(log, "Not sure why can't found any place to insert.[size=" << size << "] [old biggest_range=" << biggest_range << "] [old biggest_cap=" << biggest_cap << "] [new biggest_range=" << _biggest_range << "] [new biggest_cap=" << _biggest_cap << "]");
        biggest_range = _biggest_range;
        biggest_cap = _biggest_cap;

        return std::make_pair(offset, biggest_cap);
    }

    // Update return start
    offset = entry->start;

    if (entry->count == size)
    {
        // It is champion, need update
        if (entry->start == biggest_range)
        {
            struct rb_node * old_node = node;
            node = rb_tree_next(node);
            rb_node_remove(old_node, &rb_tree->root);
            rb_free_entry(rb_tree, entry);
            // still need update max_cap
        }
        else // It not champion, just return
        {
            rb_node_remove(node, &rb_tree->root);
            rb_free_entry(rb_tree, entry);
            max_cap = biggest_cap;
            return std::make_pair(offset, max_cap);
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
            // still need update max_cap
        }
        else // It not champion, just return
        {
            max_cap = biggest_cap;
            return std::make_pair(offset, max_cap);
        }
    }

    for (; node != nullptr; node = rb_tree_next(node))
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
    max_cap = biggest_cap;
    return std::make_pair(offset, max_cap);
}

bool RBTreeSpaceMap::markUsedImpl(UInt64 block, size_t size)
{
    bool rc;

    block -= start;

    rc = rb_remove_entry(block, size, rb_tree, log);
    rb_tree_debug(&rb_tree->root, __func__);
    return rc;
}

bool RBTreeSpaceMap::markFreeImpl(UInt64 block, size_t size)
{
    bool rc;

    block -= start;

    rc = rb_insert_entry(block, size, rb_tree, log);
    rb_tree_debug(&rb_tree->root, __func__);
    return rc;
}

bool RBTreeSpaceMap::check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker, size_t size)
{
    struct smap_rb_entry * ext;

    size_t idx = 0;
    for (struct rb_node * node = rb_tree_first(&rb_tree->root); node != nullptr; node = rb_tree_next(node))
    {
        ext = node_to_entry(node);
        if (!checker(idx, ext->start, ext->start + ext->count))
        {
            return false;
        }
        idx++;
    }

    return idx == size;
}


} // namespace DB::PS::V3
