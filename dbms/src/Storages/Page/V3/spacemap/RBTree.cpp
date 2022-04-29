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

#include <Storages/Page/V3/spacemap/RBTree.h>

#ifdef __cplusplus
extern "C" {
#endif

#define COLOR_BLACK 1
#define COLOR_MASK 3

static inline struct rb_node * node_parent(struct rb_node * node)
{
    return (struct rb_node *)(node->parent & ~COLOR_MASK);
}

static inline uintptr_t node_color(struct rb_node * node)
{
    return node->parent & COLOR_BLACK;
}

static inline void node_set_red(struct rb_node * node)
{
    node->parent &= ~COLOR_BLACK;
}

static inline void node_set_black(struct rb_node * node)
{
    node->parent |= COLOR_BLACK;
}

static inline void node_set_parent(struct rb_node * node, struct rb_node * p)
{
    node->parent = (node->parent & COLOR_MASK) | (uintptr_t)p;
}

static inline void node_set_color(struct rb_node * node, int color)
{
    node->parent = (node->parent & ~COLOR_BLACK) | color;
}

static void node_rotate_left(struct rb_node * node, struct rb_root * root)
{
    struct rb_node * right = node->node_right;
    struct rb_node * parent = node_parent(node);

    if ((node->node_right = right->node_left))
    {
        node_set_parent(right->node_left, node);
    }

    right->node_left = node;

    node_set_parent(right, parent);

    if (parent)
    {
        if (node == parent->node_left)
        {
            parent->node_left = right;
        }
        else
        {
            parent->node_right = right;
        }
    }
    else
    {
        root->rb_node = right;
    }

    node_set_parent(node, right);
}

static void node_rotate_right(struct rb_node * node, struct rb_root * root)
{
    struct rb_node * left = node->node_left;
    struct rb_node * parent = node_parent(node);

    if ((node->node_left = left->node_right))
        node_set_parent(left->node_right, node);
    left->node_right = node;

    node_set_parent(left, parent);

    if (parent)
    {
        if (node == parent->node_right)
        {
            parent->node_right = left;
        }

        else
        {
            parent->node_left = left;
        }
    }
    else
    {
        root->rb_node = left;
    }
    node_set_parent(node, left);
}

static void node_clear_color(struct rb_node * node, struct rb_node * parent, struct rb_root * root)
{
    struct rb_node * other;

    while ((!node || node_color(node)) && node != root->rb_node)
    {
        if (parent->node_left == node)
        {
            other = parent->node_right;
            if (!node_color(other))
            {
                node_set_black(other);
                node_set_red(parent);
                node_rotate_left(parent, root);
                other = parent->node_right;
            }
            if ((!other->node_left || node_color(other->node_left)) && (!other->node_right || node_color(other->node_right)))
            {
                node_set_red(other);
                node = parent;
                parent = node_parent(node);
            }
            else
            {
                if (!other->node_right || node_color(other->node_right))
                {
                    node_set_black(other->node_left);
                    node_set_red(other);
                    node_rotate_right(other, root);
                    other = parent->node_right;
                }
                node_set_color(other, node_color(parent));
                node_set_black(parent);
                node_set_black(other->node_right);
                node_rotate_left(parent, root);
                node = root->rb_node;
                break;
            }
        }
        else
        {
            other = parent->node_left;
            if (!node_color(other))
            {
                node_set_black(other);
                node_set_red(parent);
                node_rotate_right(parent, root);
                other = parent->node_left;
            }
            if ((!other->node_left || node_color(other->node_left)) && (!other->node_right || node_color(other->node_right)))
            {
                node_set_red(other);
                node = parent;
                parent = node_parent(node);
            }
            else
            {
                if (!other->node_left || node_color(other->node_left))
                {
                    node_set_black(other->node_right);
                    node_set_red(other);
                    node_rotate_left(other, root);
                    other = parent->node_left;
                }
                node_set_color(other, node_color(parent));
                node_set_black(parent);
                node_set_black(other->node_left);
                node_rotate_right(parent, root);
                node = root->rb_node;
                break;
            }
        }
    }
    if (node)
    {
        node_set_black(node);
    }
}


void rb_node_insert(struct rb_node * node, struct rb_root * root)
{
    struct rb_node *parent, *gparent;

    while ((parent = node_parent(node)) && !node_color(parent))
    {
        gparent = node_parent(parent);
        if (parent == gparent->node_left)
        {
            {
                // register
                struct rb_node * uncle = gparent->node_right;
                if (uncle && !node_color(uncle))
                {
                    node_set_black(uncle);
                    node_set_black(parent);
                    node_set_red(gparent);
                    node = gparent;
                    continue;
                }
            }

            if (parent->node_right == node)
            {
                // register
                struct rb_node * tmp;
                node_rotate_left(parent, root);
                tmp = parent;
                parent = node;
                node = tmp;
            }

            node_set_black(parent);
            node_set_red(gparent);
            node_rotate_right(gparent, root);
        }
        else
        {
            {
                // register
                struct rb_node * uncle = gparent->node_left;
                if (uncle && !node_color(uncle))
                {
                    node_set_black(uncle);
                    node_set_black(parent);
                    node_set_red(gparent);
                    node = gparent;
                    continue;
                }
            }

            if (parent->node_left == node)
            {
                // register
                struct rb_node * tmp;
                node_rotate_right(parent, root);
                tmp = parent;
                parent = node;
                node = tmp;
            }

            node_set_black(parent);
            node_set_red(gparent);
            node_rotate_left(gparent, root);
        }
    }

    node_set_black(root->rb_node);
}

void rb_node_remove(struct rb_node * node, struct rb_root * root)
{
    struct rb_node *child, *parent;
    int color;

    if (!node->node_left)
    {
        child = node->node_right;
    }
    else if (!node->node_right)
    {
        child = node->node_left;
    }
    else
    {
        struct rb_node *old = node, *left;

        node = node->node_right;
        while ((left = node->node_left) != NULL)
        {
            node = left;
        }

        if (node_parent(old))
        {
            if (node_parent(old)->node_left == old)
            {
                node_parent(old)->node_left = node;
            }
            else
            {
                node_parent(old)->node_right = node;
            }
        }
        else
        {
            root->rb_node = node;
        }

        child = node->node_right;
        parent = node_parent(node);
        color = node_color(node);

        if (parent == old)
        {
            parent = node;
        }
        else
        {
            if (child)
            {
                node_set_parent(child, parent);
            }

            parent->node_left = child;
            node->node_right = old->node_right;
            node_set_parent(old->node_right, node);
        }

        node->parent = old->parent;
        node->node_left = old->node_left;
        node_set_parent(old->node_left, node);

        goto check_color;
    }

    parent = node_parent(node);
    color = node_color(node);

    if (child)
    {
        node_set_parent(child, parent);
    }

    if (parent)
    {
        if (parent->node_left == node)
        {
            parent->node_left = child;
        }
        else
        {
            parent->node_right = child;
        }
    }
    else
    {
        root->rb_node = child;
    }

check_color:
    if (color == COLOR_BLACK)
    {
        node_clear_color(child, parent, root);
    }
}

struct rb_node * rb_tree_first(const struct rb_root * root)
{
    struct rb_node * n;

    n = root->rb_node;
    if (!n)
    {
        return NULL;
    }

    while (n->node_left)
    {
        n = n->node_left;
    }

    return n;
}

struct rb_node * rb_tree_last(const struct rb_root * root)
{
    struct rb_node * n;

    n = root->rb_node;
    if (!n)
    {
        return NULL;
    }

    while (n->node_right)
    {
        n = n->node_right;
    }

    return n;
}

struct rb_node * rb_tree_next(struct rb_node * current)
{
    struct rb_node * parent;

    // No more `next` node.
    if (node_parent(current) == current)
    {
        return NULL;
    }

    /**
     *  If tree looks like:
     *        A
     *      /   \
     *     B     C
     *    / \   /
     *   D   E F    ...
     *      /   \
     *     G     H
     *
     * Then the node is `B` , it should return `G`
     * Because `G` will bigger than `B` but small than `E`
     */
    if (current->node_right)
    {
        current = current->node_right;
        while (current->node_left)
        {
            current = current->node_left;
        }

        return current;
    }

    /**
     * In this situation, there are left-node exist.
     * And all of left node is smaller than `current` node
     * So the `next` node must in parent node,Then we need go up into the parent node.
     * If `current` node is the right-node child of its parent, Then it still need go up for upper layer.
     * 
     */
    while ((parent = node_parent(current)) && current == parent->node_right)
    {
        current = parent;
    }

    return parent;
}

struct rb_node * rb_tree_prev(struct rb_node * current)
{
    struct rb_node * parent;

    // No more `prev` node.
    if (node_parent(current) == current)
    {
        return NULL;
    }

    /**
     * Same as `rb_tree_next`
     * If tree looks like:
     *        A
     *      /   \
     *     B     C
     *      \   / \
     * ...   D E   F
     *     /     \
     *    G       H
     * Then the node is `C` , it should return `H`
     * Because `H` smaller than `C` but small than `E`
     */
    if (current->node_left)
    {
        current = current->node_left;
        while (current->node_right)
        {
            current = current->node_right;
        }
        return current;
    }

    /** 
     * Same as `rb_tree_next`
     * In this situation, there are left-node exist.
     * And all of left node is bigger than `current` node
     * So the `prev` node must in parent node,Then we need go up into the parent node.
     * If `current` node is the left-node child of its parent, Then it still need go up for upper layer.
     */
    while ((parent = node_parent(current)) && current == parent->node_left)
    {
        current = parent;
    }

    return parent;
}

void rb_tree_update_node(struct rb_node * old_node, struct rb_node * new_node, struct rb_root * root)
{
    struct rb_node * parent = node_parent(old_node);

    assert(parent != NULL);

    if (parent)
    {
        // Update the parent ptr to child
        if (old_node == parent->node_left)
        {
            parent->node_left = new_node;
        }
        else if (old_node == parent->node_right)
        {
            parent->node_right = new_node;
        }
        else
        {
            assert(false);
        }
    }
    else
    {
        /**
         * No find parent, tree is empty
         * Just update the root
         */
        root->rb_node = new_node;
    }

    // Let the `child` node point to the `new` node
    if (old_node->node_left)
    {
        node_set_parent(old_node->node_left, new_node);
    }

    if (old_node->node_right)
    {
        node_set_parent(old_node->node_right, new_node);
    }

    *new_node = *old_node;
}

#ifdef __cplusplus
} // extern "C"
#endif
