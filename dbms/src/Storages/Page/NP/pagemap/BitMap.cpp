/*
 * blkmap64_rb.c --- Simple rb-tree implementation for bitmaps
 *
 * (C)2010 Red Hat, Inc., Lukas Czerner <lczerner@redhat.com>
 * (C)2021 Pingcap, Inc., jiaqizho <zhou.jiaqi@redhat.com>
 *
 * %Begin-Header%
 * This file may be redistributed under the terms of the GNU Public
 * License.
 * %End-Header%
 */

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>

#include "Bits.h"
#include "BitMap.h"
#include "RBTree.h"
#include <Core/Types.h>

#ifdef __cplusplus
extern "C" {
#endif

struct bmap_rb_extent {
	struct rb_node node;
	UInt64 start;
	UInt64 count;
};

struct rb_private {
	struct rb_root root;
	struct bmap_rb_extent *wcursor;
	struct bmap_rb_extent *rcursor;
	struct bmap_rb_extent *rcursor_next;
};

inline static struct bmap_rb_extent *node_to_extent(struct rb_node *node)
{
	struct bmap_rb_extent *rb_ex;
	rb_ex = container_of(node, struct bmap_rb_extent, node);
	return rb_ex;
}

#ifdef NDEBUG
static void print_tree(struct rb_root *root)
{
	struct rb_node *node = NULL;
	struct bmap_rb_extent *ext;

	printf("\t\t\t=================================\n");
	node = rb_first(root);
	for (node = rb_first(root); node != NULL; node = rb_next(node)) {
		ext = node_to_extent(node);
		printf("\t\t\t--> (%llu -> %llu)\n", ext->start, ext->start + ext->count);
	}
	printf("\t\t\t=================================\n");
}

static void check_tree(struct rb_root *root, const char *msg)
{
	struct rb_node *node;
	struct bmap_rb_extent *ext, *old = NULL;

	for (node = rb_first(root); node; node = rb_next(node)) {
		ext = node_to_extent(node);
		if (ext->count == 0) {
			printf("Tree Error: count is zero\n");
			printf("extent: %llu -> %llu (%llu)\n", ext->start,
			       ext->start + ext->count, ext->count);
			goto err_out;
		}
		if (ext->start + ext->count < ext->start) {
			printf("Tree Error: start or count is crazy\n");
			printf("extent: %llu -> %llu (%llu)\n",
			       ext->start, ext->start + ext->count, ext->count);
			goto err_out;
		}

		if (old) {
			if (old->start > ext->start) {
				printf("Tree Error: start is crazy\n");
				printf("extent: %llu -> %llu (%llu)\n",
				       old->start, old->start + old->count, old->count);
				printf("extent next: %llu -> %llu (%llu)\n",
				       ext->start, ext->start + ext->count, ext->count);
				goto err_out;
			}
			if ((old->start + old->count) >= ext->start) {
				printf("Tree Error: extent is crazy\n");
				printf("extent: %llu -> %llu (%llu)\n",
				       old->start, old->start + old->count, old->count);
				printf("extent next: %llu -> %llu (%llu)\n",
				       ext->start, ext->start + ext->count, ext->count);
				goto err_out;
			}
		}
		old = ext;
	}
	return;

      err_out:
	printf("%s\n", msg);
	print_tree(root);
	exit(1);
}
#else
#define check_tree(root, msg) do {} while (0)
#define print_tree(root) do {} while (0)
#endif

static void rb_get_new_extent(struct bmap_rb_extent **ext, UInt64 start, UInt64 count)
{
	struct bmap_rb_extent *new_ext;

	new_ext = (struct bmap_rb_extent *)calloc(1, sizeof(struct bmap_rb_extent));
	if (new_ext == NULL)
		abort();

	new_ext->start = start;
	new_ext->count = count;
	*ext = new_ext;
}

inline static void rb_free_extent(struct rb_private *bp, struct bmap_rb_extent *ext)
{
	if (bp->wcursor == ext)
		bp->wcursor = NULL;
	if (bp->rcursor == ext)
		bp->rcursor = NULL;
	if (bp->rcursor_next == ext)
		bp->rcursor_next = NULL;
	free(ext);
}

static int rb_alloc_private_data(struct bitmaps *bitmap)
{
	struct rb_private *bp;

	bp = (struct rb_private *)calloc(1, sizeof(struct rb_private));
	if (bp == NULL)
		return -1;

	bp->root = RB_ROOT;
	bp->rcursor = NULL;
	bp->rcursor_next = NULL;
	bp->wcursor = NULL;

	bitmap->_private = (void *)bp;
	return 0;
}

static int rb_new_bmap(struct bitmaps *bitmap)
{
	int ret;

	ret = rb_alloc_private_data(bitmap);
	if (ret)
		return ret;

	return 0;
}

static void rb_free_tree(struct rb_root *root)
{
	struct bmap_rb_extent *ext;
	struct rb_node *node, *next;

	for (node = rb_first(root); node; node = next) {
		next = rb_next(node);
		ext = node_to_extent(node);
		rb_erase(node, root);
		free(ext);
	}
}

static void rb_free_bmap(struct bitmaps *bitmap)
{
	struct rb_private *bp;

	bp = (struct rb_private *)bitmap->_private;

	rb_free_tree(&bp->root);
	free(bp);
}

static int rb_insert_extent(UInt64 start, UInt64 count, struct rb_private *bp)
{
	struct rb_root *root = &bp->root;
	struct rb_node *parent = NULL, **n = &root->rb_node;
	struct rb_node *new_node, *node, *next;
	struct bmap_rb_extent *new_ext;
	struct bmap_rb_extent *ext;
	int retval = 0;

	if (count == 0)
		return 0;

	bp->rcursor_next = NULL;
	ext = bp->wcursor;
	if (ext) {
		if (start >= ext->start && start <= (ext->start + ext->count)) {
			goto got_extent;
		}
	}

	while (*n) {
		parent = *n;
		ext = node_to_extent(parent);

		if (start < ext->start) {
			n = &(*n)->rb_left;
		} else if (start > (ext->start + ext->count)) {
			n = &(*n)->rb_right;
		} else {
      got_extent:
			if ((start + count) <= (ext->start + ext->count))
				return 1;

			if ((ext->start + ext->count) == start)
				retval = 0;
			else
				retval = 1;

			count += (start - ext->start);
			start = ext->start;
			new_ext = ext;
			new_node = &ext->node;

			goto skip_insert;
		}
	}

	rb_get_new_extent(&new_ext, start, count);

	new_node = &new_ext->node;
	rb_link_node(new_node, parent, n);
	rb_insert_color(new_node, root);
	bp->wcursor = new_ext;

	node = rb_prev(new_node);
	if (node) {
		ext = node_to_extent(node);
		if ((ext->start + ext->count) == start) {
			start = ext->start;
			count += ext->count;
			rb_erase(node, root);
			rb_free_extent(bp, ext);
		}
	}

      skip_insert:
	/* See if we can merge extent to the right */
	for (node = rb_next(new_node); node != NULL; node = next) {
		next = rb_next(node);
		ext = node_to_extent(node);

		if ((ext->start + ext->count) <= start)
			continue;

		/* No more merging */
		if ((start + count) < ext->start)
			break;

		/* ext is embedded in new_ext interval */
		if ((start + count) >= (ext->start + ext->count)) {
			rb_erase(node, root);
			rb_free_extent(bp, ext);
			continue;
		} else {
			/* merge ext with new_ext */
			count += ((ext->start + ext->count) - (start + count));
			rb_erase(node, root);
			rb_free_extent(bp, ext);
			break;
		}
	}

	new_ext->start = start;
	new_ext->count = count;

	return retval;
}

static int rb_mark_bmap(struct bitmaps *bitmap, UInt64 arg)
{
	struct rb_private *bp;
	int retval;

	bp = (struct rb_private *)bitmap->_private;
	arg -= bitmap->start;

	retval = rb_insert_extent(arg, 1, bp);
	check_tree(&bp->root, __func__);
	return retval;
}

static int rb_remove_extent(UInt64 start, UInt64 count, struct rb_private *bp)
{
	struct rb_root *root = &bp->root;
	struct rb_node *parent = NULL, **n = &root->rb_node;
	struct rb_node *node;
	struct bmap_rb_extent *ext;
	UInt64 new_start, new_count;
	int retval = 0;

	if (rb_empty_root(root))
		return 0;

	while (*n) {
		parent = *n;
		ext = node_to_extent(parent);
		if (start < ext->start) {
			n = &(*n)->rb_left;
			continue;
		} else if (start >= (ext->start + ext->count)) {
			n = &(*n)->rb_right;
			continue;
		}

		if ((start > ext->start) && (start + count) < (ext->start + ext->count)) {
			/* We have to split extent into two */
			new_start = start + count;
			new_count = (ext->start + ext->count) - new_start;

			ext->count = start - ext->start;

			rb_insert_extent(new_start, new_count, bp);
			return 1;
		}

		if ((start + count) >= (ext->start + ext->count)) {
			ext->count = start - ext->start;
			retval = 1;
		}

		if (0 == ext->count) {
			parent = rb_next(&ext->node);
			rb_erase(&ext->node, root);
			rb_free_extent(bp, ext);
			break;
		}

		if (start == ext->start) {
			ext->start += count;
			ext->count -= count;
			return 1;
		}
	}

	/* See if we should delete or truncate extent on the right */
	for (; parent != NULL; parent = node) {
		node = rb_next(parent);
		ext = node_to_extent(parent);
		if ((ext->start + ext->count) <= start)
			continue;

		/* No more extents to be removed/truncated */
		if ((start + count) < ext->start)
			break;

		/* The entire extent is within the region to be removed */
		if ((start + count) >= (ext->start + ext->count)) {
			rb_erase(parent, root);
			rb_free_extent(bp, ext);
			retval = 1;
			continue;
		} else {
			/* modify the last extent in region to be removed */
			ext->count -= ((start + count) - ext->start);
			ext->start = start + count;
			retval = 1;
			break;
		}
	}

	return retval;
}

static int rb_unmark_bmap(struct bitmaps *bitmap, UInt64 arg)
{
	struct rb_private *bp;
	int retval;

	bp = (struct rb_private *)bitmap->_private;
	arg -= bitmap->start;

	retval = rb_remove_extent(arg, 1, bp);
	check_tree(&bp->root, __func__);

	return retval;
}

static void rb_mark_bmap_extent(struct bitmaps *bitmap, UInt64 arg, unsigned int num)
{
	struct rb_private *bp;

	bp = (struct rb_private *)bitmap->_private;
	arg -= bitmap->start;

	rb_insert_extent(arg, num, bp);
	check_tree(&bp->root, __func__);
}

static void rb_unmark_bmap_extent(struct bitmaps *bitmap, UInt64 arg, unsigned int num)
{
	struct rb_private *bp;

	bp = (struct rb_private *)bitmap->_private;
	arg -= bitmap->start;

	rb_remove_extent(arg, num, bp);
	check_tree(&bp->root, __func__);
}

static void rb_clear_bmap(struct bitmaps *bitmap)
{
	struct rb_private *bp;

	bp = (struct rb_private *)bitmap->_private;

	rb_free_tree(&bp->root);
	bp->rcursor = NULL;
	bp->rcursor_next = NULL;
	bp->wcursor = NULL;
	check_tree(&bp->root, __func__);
}

inline static int rb_test_bit(struct rb_private *bp, UInt64 bit)
{
	struct bmap_rb_extent *rcursor, *next_ext = NULL;
	struct rb_node *parent = NULL, *next;
	struct rb_node **n = &bp->root.rb_node;
	struct bmap_rb_extent *ext;

	rcursor = bp->rcursor;
	if (!rcursor)
		goto search_tree;

	if (bit >= rcursor->start && bit < rcursor->start + rcursor->count) {
		return 1;
	}

	next_ext = bp->rcursor_next;
	if (!next_ext) {
		next = rb_next(&rcursor->node);
		if (next)
			next_ext = node_to_extent(next);
		bp->rcursor_next = next_ext;
	}
	if (next_ext) {
		if ((bit >= rcursor->start + rcursor->count) && (bit < next_ext->start)) {
			return 0;
		}
	}
	bp->rcursor = NULL;
	bp->rcursor_next = NULL;

	rcursor = bp->wcursor;
	if (!rcursor)
		goto search_tree;

	if (bit >= rcursor->start && bit < rcursor->start + rcursor->count)
		return 1;

      search_tree:

	while (*n) {
		parent = *n;
		ext = node_to_extent(parent);
		if (bit < ext->start)
			n = &(*n)->rb_left;
		else if (bit >= (ext->start + ext->count))
			n = &(*n)->rb_right;
		else {
			bp->rcursor = ext;
			bp->rcursor_next = NULL;
			return 1;
		}
	}
	return 0;
}

inline static int rb_test_bmap(struct bitmaps *bitmap, UInt64 arg)
{
	struct rb_private *bp;

	bp = (struct rb_private *)bitmap->_private;
	arg -= bitmap->start;

	return rb_test_bit(bp, arg);
}

static int rb_test_clear_bmap_extent(struct bitmaps *bitmap,
				     UInt64 start, unsigned int len)
{
	struct rb_node *parent = NULL, **n;
	struct rb_node *node, *next;
	struct rb_private *bp;
	struct bmap_rb_extent *ext;
	int retval = 1;

	bp = (struct rb_private *)bitmap->_private;
	n = &bp->root.rb_node;
	start -= bitmap->start;

	if (len == 0 || rb_empty_root(&bp->root))
		return 1;

	/*
	 * If we find nothing, we should examine whole extent, but
	 * when we find match, the extent is not clean, thus be return
	 * false.
	 */
	while (*n) {
		parent = *n;
		ext = node_to_extent(parent);
		if (start < ext->start) {
			n = &(*n)->rb_left;
		} else if (start >= (ext->start + ext->count)) {
			n = &(*n)->rb_right;
		} else {
			/*
			 * We found extent int the tree -> extent is not
			 * clean
			 */
			return 0;
		}
	}

	node = parent;
	while (node) {
		next = rb_next(node);
		ext = node_to_extent(node);
		node = next;

		if ((ext->start + ext->count) <= start)
			continue;

		/* No more merging */
		if ((start + len) <= ext->start)
			break;

		retval = 0;
		break;
	}
	return retval;
}

static void rb_print_stats(struct bitmaps *bitmap)
{
	struct rb_private *bp;
	struct rb_node *node = NULL;
	struct bmap_rb_extent *ext;
	UInt64 count = 0;
	UInt64 max_size = 0;
	UInt64 min_size = ULONG_MAX;
	UInt64 size = 0, avg_size = 0;
	double eff;

	bp = (struct rb_private *)bitmap->_private;
	printf("extend status :\n");
	for (node = rb_first(&bp->root); node != NULL; node = rb_next(node)) {
		ext = node_to_extent(node);
		printf("ext%lld : start=%lld , num=%lld \n", count, ext->start, ext->count);
		count++;
		if (ext->count > max_size)
			max_size = ext->count;
		if (ext->count < min_size)
			min_size = ext->count;
		size += ext->count;
	}

	if (count)
		avg_size = size / count;
	if (min_size == ULONG_MAX)
		min_size = 0;
	eff = (double)((count * sizeof(struct bmap_rb_extent)) << 3) /
	    (bitmap->real_end - bitmap->start);

	printf("%16llu extents (%llu bytes)\n", count,
	       ((count * sizeof(struct bmap_rb_extent)) + sizeof(struct rb_private)));
	printf("%16llu bits minimum size\n", min_size);
	printf("%16llu bits maximum size\n" "%16llu bits average size\n", max_size, avg_size);
	printf("%16llu bits set in bitmap (out of %llu)\n", size,
	       bitmap->real_end - bitmap->start);
	printf("%16.4lf memory / bitmap bit memory ratio (bitarray = 1)\n", eff);
}

static int rb_set_bmap_range(struct bitmaps *bitmap, UInt64 start, size_t num, void *in)
{
	struct rb_private *bp;
	unsigned char *cp = (unsigned char *)in;
	size_t i;
	int first_set = -1;

	bp = (struct rb_private *)bitmap->_private;

	for (i = 0; i < num; i++) {
		if ((i & 7) == 0) {
			unsigned char c = cp[i / 8];
			if (c == 0xFF) {
				if (first_set == -1)
					first_set = i;
				i += 7;
				continue;
			}
			if ((c == 0x00) && (first_set == -1)) {
				i += 7;
				continue;
			}
		}
		if (test_bit(i, in)) {
			if (first_set == -1)
				first_set = i;
			continue;
		}
		if (first_set == -1)
			continue;

		rb_insert_extent(start + first_set - bitmap->start, i - first_set, bp);
		check_tree(&bp->root, __func__);
		first_set = -1;
	}
	if (first_set != -1) {
		rb_insert_extent(start + first_set - bitmap->start, num - first_set, bp);
		check_tree(&bp->root, __func__);
	}

	return 0;
}

static int rb_get_bmap_range(struct bitmaps *bitmap, UInt64 start, size_t num, void *out)
{
	struct rb_node *parent = NULL, *next, **n;
	struct rb_private *bp;
	struct bmap_rb_extent *ext;
	UInt64 count, pos;

	bp = (struct rb_private *)bitmap->_private;
	n = &bp->root.rb_node;
	start -= bitmap->start;

	if (rb_empty_root(&bp->root))
		return 0;

	while (*n) {
		parent = *n;
		ext = node_to_extent(parent);
		if (start < ext->start) {
			n = &(*n)->rb_left;
		} else if (start >= (ext->start + ext->count)) {
			n = &(*n)->rb_right;
		} else
			break;
	}

	memset(out, 0, (num + 7) >> 3);

	for (; parent != NULL; parent = next) {
		next = rb_next(parent);
		ext = node_to_extent(parent);

		pos = ext->start;
		count = ext->count;
		if (pos >= start + num)
			break;
		if (pos < start) {
			if (pos + count < start)
				continue;
			count -= start - pos;
			pos = start;
		}
		if (pos + count > start + num)
			count = start + num - pos;

		while (count > 0) {
			if ((count >= 8) && ((pos - start) % 8) == 0) {
				int nbytes = count >> 3;
				int offset = (pos - start) >> 3;

				memset(((char *)out) + offset, 0xFF, nbytes);
				pos += nbytes << 3;
				count -= nbytes << 3;
				continue;
			}
			set_bit64((pos - start), out);
			pos++;
			count--;
		}
	}
	return 0;
}

struct bitmap_ops bmap_rbtree = {
	.type = BMAP64_RBTREE,
	.new_bmap = rb_new_bmap,
	.free_bmap = rb_free_bmap,
	.mark_bmap = rb_mark_bmap,
	.unmark_bmap = rb_unmark_bmap,
	.mark_bmap_extent = rb_mark_bmap_extent,
	.unmark_bmap_extent = rb_unmark_bmap_extent,
	.test_bmap = rb_test_bmap,
	.test_clear_bmap_extent = rb_test_clear_bmap_extent,
	.set_bmap_range = rb_set_bmap_range,
	.get_bmap_range = rb_get_bmap_range,
	.print_stats = rb_print_stats,
	.clear_bmap = rb_clear_bmap
};

int unmark_bmap(struct bitmaps *bitmap, UInt64 arg)
{
	if (!bitmap) {
		return -1;
	}
	arg >>= bitmap->cluster_bits;

	if ((arg < bitmap->start) || (arg > bitmap->end)) {
		return -1;
	}

	return bitmap->bitmap_ops->unmark_bmap(bitmap, arg);
}

int mark_bmap(struct bitmaps *bitmap, UInt64 arg)
{
	if (!bitmap) {
		return -1;
	}

	arg >>= bitmap->cluster_bits;

	if ((arg < bitmap->start) || (arg > bitmap->end)) {
		return -1;
	}

	return bitmap->bitmap_ops->mark_bmap(bitmap, arg);
}

int unmark_block_bmap_range(struct bitmaps *bitmap, UInt64 block, unsigned int num)
{
	UInt64 end = block + num;

	if (!bitmap) {
		return -1;
	}

	/* convert to clusters if necessary */
	block >>= bitmap->cluster_bits;
	end += (1 << bitmap->cluster_bits) - 1;
	end >>= bitmap->cluster_bits;
	num = end - block;

	if ((block < bitmap->start) || (block > bitmap->end) ||
	    (block + num - 1 > bitmap->end)) {
		return -1;
	}

	bitmap->bitmap_ops->unmark_bmap_extent(bitmap, block, num);
	return 0;
}

int mark_block_bmap_range(struct bitmaps *bitmap, UInt64 block, unsigned int num)
{
	UInt64 end = block + num;

	if (!bitmap) {
		return -1;
	}
	/* convert to clusters if necessary */
	block >>= bitmap->cluster_bits;
	end += (1 << bitmap->cluster_bits) - 1;
	end >>= bitmap->cluster_bits;
	num = end - block;

	if ((block < bitmap->start) || (block > bitmap->end) ||
	    (block + num - 1 > bitmap->end)) {
		return -1;
	}

	bitmap->bitmap_ops->mark_bmap_extent(bitmap, block, num);
	return 0;
}

int test_bmap(struct bitmaps *bitmap, UInt64 arg)
{
	if (!bitmap)
		return 0;

	arg >>= bitmap->cluster_bits;

	if ((arg < bitmap->start) || (arg > bitmap->end)) {
		return -1;
	}

	return bitmap->bitmap_ops->test_bmap(bitmap, arg);
}

int test_bmap_range(struct bitmaps *bitmap, UInt64 block, unsigned int num)
{
	UInt64 end = block + num;

	if (num == 1)
		return !test_bmap(bitmap, block);

	block >>= bitmap->cluster_bits;
	end += (1 << bitmap->cluster_bits) - 1;
	end >>= bitmap->cluster_bits;
	num = end - block;

	if ((block < bitmap->start) || (block > bitmap->end) ||
	    (block + num - 1 > bitmap->end)) {
		return -1;
	}

	return bitmap->bitmap_ops->test_clear_bmap_extent(bitmap, block, num);
}

int get_free_blocks(struct bitmaps *bitmap, UInt64 start, UInt64 finish,
			 int num, UInt64 * ret)
{
	int c_ratio;
	int _start;
	assert(bitmap != NULL);
	assert(finish > start);

	_start = start;

	c_ratio = 1 << bitmap->cluster_bits;
	// todo cluster not support for now
	assert(c_ratio == 1);

	do {
		if (start + num - 1 >= bitmap->end) {
			return -1;
		}
		if (test_bmap_range(bitmap, start, num)) {
			*ret = start;
			return 0;
		}
		start += c_ratio;
	} while (start != finish);
	
	return -1;
}

int set_bmap_range(struct bitmaps *bitmap, UInt64 start, unsigned int num, void *in)
{
	if ((start < bitmap->start) || (start > bitmap->end) ||
	    (start + num - 1 > bitmap->end)) {
		return -1;
	}

	return bitmap->bitmap_ops->set_bmap_range(bitmap, start, num, in);
}

int get_bmap_range(struct bitmaps *bitmap, UInt64 start, unsigned int num, void *out)
{
	if ((start < bitmap->start) || (start > bitmap->end) ||
	    (start + num - 1 > bitmap->end)) {
		return -1;
	}

	return bitmap->bitmap_ops->get_bmap_range(bitmap, start, num, out);
}

int init_bitmaps(struct bitmaps *bitmap, int type, UInt64 start, UInt64 end, UInt64 real_end)
{
	int rc = 0;
	assert(bitmap != NULL);

	switch (type){
		case BMAP64_RBTREE:
			break;
		case BMAP64_BITARRAY:
		case BMAP64_AUTODIR: // not support yet
			return -1;
	}

	bitmap->bitmap_ops = &bmap_rbtree;
	bitmap->start = start;
	bitmap->end = end;
	bitmap->real_end = real_end;
	bitmap->cluster_bits = 0;

	rc = bitmap->bitmap_ops->new_bmap(bitmap);
	if (rc != 0) 
	{
		bitmap->bitmap_ops->free_bmap(bitmap);
	}

	return rc;
}

void free_bitmaps(struct bitmaps *bitmap)
{
	if (bitmap != NULL)
		bitmap->bitmap_ops->free_bmap(bitmap);
}


#ifdef __cplusplus
} // extern "C"
#endif
