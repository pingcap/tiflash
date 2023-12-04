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

#include <Common/Exception.h>
#include <Storages/Page/V3/spacemap/SpaceMapRBTree.h>

namespace DB::PS::V3
{
struct SmapRbEntry
{
    struct rb_node node;
    UInt64 start;
    UInt64 count;
};

struct RbPrivate
{
    struct rb_root root;
    // Cache the index for write
    struct SmapRbEntry * write_index;
    // Cache the index for read
    struct SmapRbEntry * read_index;
    struct SmapRbEntry * read_index_next;
};

// convert rb_node to SmapRbEntry
inline static struct SmapRbEntry * node_to_entry(struct rb_node * node)
{
    return reinterpret_cast<SmapRbEntry *>(node);
}

static bool rb_insert_entry(UInt64 start, UInt64 count, struct RbPrivate * private_data, Poco::Logger * log);
static bool rb_remove_entry(UInt64 start, UInt64 count, struct RbPrivate * private_data, Poco::Logger * log);

static inline void rb_link_node(struct rb_node * node,
                                struct rb_node * parent,
                                struct rb_node ** rb_link)
{
    node->parent = reinterpret_cast<uintptr_t>(parent);
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
    struct SmapRbEntry * entry;

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


static void rb_get_new_entry(struct SmapRbEntry ** entry, UInt64 start, UInt64 count)
{
    struct SmapRbEntry * new_entry;

    new_entry = static_cast<struct SmapRbEntry *>(calloc(1, sizeof(struct SmapRbEntry))); // NOLINT
    if (new_entry == nullptr)
    {
        return;
    }

    new_entry->start = start;
    new_entry->count = count;
    *entry = new_entry;
}

inline static void rb_free_entry(struct RbPrivate * private_data, struct SmapRbEntry * entry)
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

    free(entry); // NOLINT
}


static bool rb_insert_entry(UInt64 start, UInt64 count, struct RbPrivate * private_data, Poco::Logger * log)
{
    struct rb_root * root = &private_data->root;
    struct rb_node *parent = nullptr, **n = &root->rb_node;
    struct rb_node *new_node, *node, *next;
    struct SmapRbEntry * new_entry = nullptr;
    struct SmapRbEntry * entry = nullptr;
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
                    auto * n_node = rb_tree_next(parent);
                    if (n_node)
                    {
                        auto * n_entry = node_to_entry(n_node);
                        if (start + count > n_entry->start)
                        {
                            LOG_FMT_WARNING(log, "Marked space free failed. [offset={}, size={}], next node is [offset={},size={}]", start, count, n_entry->start, n_entry->count);
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
    if (!new_entry)
    {
        LOG_WARNING(log, "No enough memory");
        return false;
    }

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
            LOG_FMT_WARNING(log, "Marked space free failed. [offset={}, size={}], prev node is [offset={},size={}]", new_entry->start, new_entry->count, entry->start, entry->count);
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
            LOG_FMT_WARNING(log, "Marked space free failed. [offset={}, size={}], next node is [offset={},size={}]", new_entry->start, new_entry->count, entry->start, entry->count);
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


static bool rb_remove_entry(UInt64 start, UInt64 count, struct RbPrivate * private_data, Poco::Logger * log)
{
    struct rb_root * root = &private_data->root;
    struct rb_node *parent = nullptr, **n = &root->rb_node;
    struct rb_node * node = nullptr;
    struct SmapRbEntry * entry = nullptr;
    UInt64 new_start, new_count;
    bool marked = false;

    // Root node have not been init
    if (private_data->root.rb_node == nullptr)
    {
        LOG_ERROR(log, "Current spacemap is invalid.");
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
            LOG_FMT_WARNING(log, "Marked space used failed. [offset={}, size={}] is bigger than space [offset={},size={}]", start, count, entry->start, entry->count);
            return false;
        }

        if (start < entry->start)
        {
            LOG_FMT_WARNING(log, "Marked space used failed. [offset={}, size={}] is less than space [offset={},size={}]", start, count, entry->start, entry->count);
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

    ptr->rb_tree = static_cast<struct RbPrivate *>(calloc(1, sizeof(struct RbPrivate))); // NOLINT
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
        LOG_FMT_ERROR(ptr->log, "Erorr happend, when mark all space free.  [start={}] , [end={}]", start, end);
        free(ptr->rb_tree); // NOLINT
        return nullptr;
    }
    return ptr;
}

static void rb_free_tree(struct rb_root * root)
{
    struct SmapRbEntry * entry;
    struct rb_node *node, *next;

    for (node = rb_tree_first(root); node; node = next)
    {
        next = rb_tree_next(node);
        entry = node_to_entry(node);
        rb_node_remove(node, root);
        free(entry); // NOLINT
    }
}

void RBTreeSpaceMap::freeSmap()
{
    if (rb_tree)
    {
        rb_free_tree(&rb_tree->root);
        free(rb_tree); // NOLINT
    }
}

String RBTreeSpaceMap::toDebugString()
{
    struct rb_node * node = nullptr;
    struct SmapRbEntry * entry;
    UInt64 count = 0;
    FmtBuffer fmt_buffer;

    if (rb_tree->root.rb_node == nullptr)
    {
        fmt_buffer.append("Tree have not been inited.");
        return fmt_buffer.toString();
    }

    fmt_buffer.append("    RB-Tree entries status: \n");
    for (node = rb_tree_first(&rb_tree->root); node != nullptr; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        fmt_buffer.fmtAppend("      Space: {} start: {} size: {} \n", count, entry->start, entry->count);
        count++;
    }

    return fmt_buffer.toString();
}

bool RBTreeSpaceMap::isMarkUnused(UInt64 offset, size_t length)
{
    struct rb_node *parent = nullptr, **n;
    struct rb_node *node, *next;
    struct SmapRbEntry * entry;
    bool retval = false;

    n = &rb_tree->root.rb_node;
    offset -= start;

    if (length == 0 || rb_tree->root.rb_node == nullptr)
    {
        LOG_ERROR(log, "Current spacemap is invalid.");
    }

    while (*n)
    {
        parent = *n;
        entry = node_to_entry(parent);
        if (offset < entry->start)
        {
            n = &(*n)->node_left;
        }
        else if (offset >= (entry->start + entry->count))
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

        if ((entry->start + entry->count) <= offset)
            continue;

        /* No more merging */
        if ((offset + length) <= entry->start)
            break;

        retval = true;
        break;
    }
    return retval;
}

std::tuple<UInt64, UInt64, bool> RBTreeSpaceMap::searchInsertOffset(size_t size)
{
    UInt64 offset = UINT64_MAX, last_offset = UINT64_MAX;
    UInt64 max_cap = 0;
    struct rb_node *node = nullptr, *last_node = nullptr;
    struct SmapRbEntry *entry, *last_entry;

    UInt64 scan_biggest_cap = 0;
    UInt64 scan_biggest_offset = 0;

    node = rb_tree_first(&rb_tree->root);
    if (node == nullptr)
    {
        LOG_ERROR(log, "Current spacemap is full.");
        biggest_cap = 0;
        return std::make_tuple(offset, biggest_cap, false);
    }

    last_node = rb_tree_last(&rb_tree->root);
    if (last_node != nullptr)
    {
        last_entry = node_to_entry(last_node);
        last_offset = (last_entry->start + last_entry->count == end) ? last_entry->start : UINT64_MAX;
    }
    else
    {
        LOG_ERROR(log, "Current spacemap is invalid.");
    }

    for (; node != nullptr; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        if (entry->count >= size)
        {
            break;
        }
        else
        {
            if (entry->count > scan_biggest_cap)
            {
                scan_biggest_cap = entry->count;
                scan_biggest_offset = entry->start;
            }
        }
    }

    // No enough space for insert
    if (!node)
    {
        LOG_FMT_ERROR(
            log,
            "Not sure why can't found any place to insert.[size={}] [old biggest_range={}] [old biggest_cap={}] [new biggest_range={}] [new biggest_cap={}]",
            size,
            biggest_range,
            biggest_cap,
            scan_biggest_offset,
            scan_biggest_cap);
        biggest_range = scan_biggest_offset;
        biggest_cap = scan_biggest_cap;

        return std::make_tuple(offset, biggest_cap, false);
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
            return std::make_tuple(offset, max_cap, offset == last_offset);
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
            if (entry->count > scan_biggest_cap)
            {
                scan_biggest_cap = entry->count;
                scan_biggest_offset = entry->start;
            }
            node = rb_tree_next(node);
            // still need update max_cap
        }
        else // It not champion, just return
        {
            max_cap = biggest_cap;
            return std::make_tuple(offset, max_cap, offset == last_offset);
        }
    }

    for (; node != nullptr; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        if (entry->count > scan_biggest_cap)
        {
            scan_biggest_cap = entry->count;
            scan_biggest_offset = entry->start;
        }
    }
    biggest_range = scan_biggest_offset;
    biggest_cap = scan_biggest_cap;
    max_cap = biggest_cap;
    return std::make_tuple(offset, max_cap, offset == last_offset);
}

UInt64 RBTreeSpaceMap::updateAccurateMaxCapacity()
{
    struct rb_node * node = nullptr;
    struct SmapRbEntry * entry;
    UInt64 max_offset = 0;
    UInt64 max_cap = 0;

    node = rb_tree_first(&rb_tree->root);
    if (node == nullptr)
    {
        return max_cap;
    }

    for (; node != nullptr; node = rb_tree_next(node))
    {
        entry = node_to_entry(node);
        if (entry->count > max_cap)
        {
            max_offset = entry->start;
            max_cap = entry->count;
        }
    }

    biggest_range = max_offset;
    biggest_cap = max_cap;
    return max_cap;
}

std::pair<UInt64, UInt64> RBTreeSpaceMap::getSizes() const
{
    struct rb_node * node = rb_tree_last(&rb_tree->root);
    if (node == nullptr)
    {
        auto range = end - start;
        return std::make_pair(range, range);
    }

    auto * entry = node_to_entry(node);
    if (entry->start + entry->count != end)
    {
        UInt64 total_size = end - start;
        UInt64 valid_size = total_size;
        for (node = rb_tree_first(&rb_tree->root); node != nullptr; node = rb_tree_next(node))
        {
            entry = node_to_entry(node);
            valid_size -= entry->count;
        }

        return std::make_pair(total_size, valid_size);
    }
    else
    {
        UInt64 total_size = entry->start - start;
        UInt64 last_node_size = entry->count;
        UInt64 valid_size = 0;

        for (node = rb_tree_first(&rb_tree->root); node != nullptr; node = rb_tree_next(node))
        {
            entry = node_to_entry(node);
            valid_size += entry->count;
        }
        valid_size = total_size - (valid_size - last_node_size);

        return std::make_pair(total_size, valid_size);
    }
}

UInt64 RBTreeSpaceMap::getUsedBoundary()
{
    struct rb_node * node = rb_tree_last(&rb_tree->root);
    if (node == nullptr)
    {
        return end;
    }

    auto * entry = node_to_entry(node);

    // If the `offset+size` of the last free node is not equal to `end`, it means the range `[last_node.offset, end)` is marked as used,
    // then we should return `end` as the used boundary.
    //
    // eg.
    //  1. The spacemap manage a space of `[0, 100]`
    //  2. A span {offset=90, size=10} is marked as used, then the free range in SpaceMap is `[0, 90)`
    //  3. The return value should be 100
    if (entry->start + entry->count != end)
    {
        return end;
    }

    // Else we should return the offset of last free node
    return entry->start;
}

bool RBTreeSpaceMap::markUsedImpl(UInt64 offset, size_t length)
{
    offset -= start;

    bool rc = rb_remove_entry(offset, length, rb_tree, log);
    rb_tree_debug(&rb_tree->root, __func__);
    return rc;
}

bool RBTreeSpaceMap::markFreeImpl(UInt64 offset, size_t length)
{
    offset -= start;

    bool rc = rb_insert_entry(offset, length, rb_tree, log);
    rb_tree_debug(&rb_tree->root, __func__);
    return rc;
}

bool RBTreeSpaceMap::check(std::function<bool(size_t idx, UInt64 start, UInt64 end)> checker, size_t size)
{
    struct SmapRbEntry * ext;

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
