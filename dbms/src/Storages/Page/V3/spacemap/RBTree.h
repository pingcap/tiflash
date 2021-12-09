#pragma once

#include <Core/Types.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

struct rb_node
{
    /**
     * using uintptr_t 
     * - contain node color 
     * - contain parent ptr
     */
    uintptr_t parent;
    struct rb_node * node_right;
    struct rb_node * node_left;
} __attribute__((aligned(sizeof(long))));

struct rb_root
{
    struct rb_node * rb_node;
};

/**
 * Insert node into red-black tree
 */
void rb_node_insert(struct rb_node *, struct rb_root *);

/**
 * remove node from red-black tree
 */
void rb_node_remove(struct rb_node *, struct rb_root *);

/**
 * Return the first node of the tree.
 * It is a O(n) method
 * call the `rb_next` as a iterator
 */
struct rb_node * rb_tree_first(const struct rb_root *);

/**
 * Return the last node of the tree.
 *  It is a O(n) method
 *  call the `rb_prev` as a iterator
 */
struct rb_node * rb_tree_last(const struct rb_root *);

/**
 * Return the next node of the tree.
 */
struct rb_node * rb_tree_next(struct rb_node *);

/**
 * Return the prev node of the tree.
 */
struct rb_node * rb_tree_prev(struct rb_node *);

/**
 * Update node into a new node
 * - Just replace the position, tree won't rotate
 * - Note its own pointer
 */
void rb_tree_update_node(struct rb_node * old_node, struct rb_node * new_node, struct rb_root * root);

#ifdef __cplusplus
} // extern "C"
#endif
