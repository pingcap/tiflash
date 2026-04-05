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
DT_TEMPLATE
template <class T>
__attribute__((noinline, flatten)) typename DT_CLASS::InternPtr DT_CLASS::TIFLASH_DT_IMPL_NAME(T * node)
{
    if (!node)
        return {};

    constexpr bool is_leaf = std::is_same<Leaf, T>::value;

    if (root == asNode(node) && !isLeaf(root) && node->count == 1)
    {
        /// Decrease tree height.
        root = as(Intern, root)->children[0];

        --(node->count);
        freeNode<T>(node);

        if (isLeaf(root))
            as(Leaf, root)->parent = nullptr;
        else
            as(Intern, root)->parent = nullptr;
        --height;

        return {};
    }

    auto parent = node->parent;
    bool parent_updated = false;

    if (T::overflow(node->count)) // split
    {
        if (!parent)
        {
            /// Increase tree height.
            parent = createNode<Intern>();
            root = asNode(parent);

            parent->deltas[0] = checkDelta(node->getDelta());
            parent->children[0] = asNode(node);
            ++(parent->count);
            parent->refreshChildParent();

            ++height;

        }

        auto pos = parent->searchChild(asNode(node));

        T * next_n = createNode<T>();

        UInt64 sep_sid = node->split(next_n);

        // handle parent update
        parent->shiftEntries(pos + 1, 1);
        // for current node
        parent->deltas[pos] = checkDelta(node->getDelta());
        // for next node
        parent->sids[pos] = sep_sid;
        parent->deltas[pos + 1] = checkDelta(next_n->getDelta());
        parent->children[pos + 1] = asNode(next_n);

        ++(parent->count);

        if constexpr (is_leaf)
        {
            if (as(Leaf, node) == right_leaf)
                right_leaf = as(Leaf, next_n);
        }

        parent_updated = true;
    }
    else if (T::underflow(node->count) && root != asNode(node)) // adopt or merge
    {
        auto pos = parent->searchChild(asNode(node));

        // currently we always adopt from the right one if possible
        bool is_sibling_left;
        size_t sibling_pos;
        T * sibling;

        if (unlikely(parent->count <= 1))
            throw Exception("Unexpected parent entry count: " + DB::toString(parent->count));

        if (pos == parent->count - 1)
        {
            is_sibling_left = true;
            sibling_pos = pos - 1;
            sibling = as(T, parent->children[sibling_pos]);
        }
        else
        {
            is_sibling_left = false;
            sibling_pos = pos + 1;
            sibling = as(T, parent->children[sibling_pos]);
        }

        if (unlikely(sibling->parent != node->parent))
            throw Exception("parent not the same");

        auto after_adopt = (node->count + sibling->count) / 2;
        if (T::underflow(after_adopt))
        {
            // Do merge.
            // adoption won't work because the sibling doesn't have enough entries.

            node->merge(sibling, is_sibling_left, pos);
            freeNode<T>(sibling);

            pos = std::min(pos, sibling_pos);
            parent->deltas[pos] = checkDelta(node->getDelta());
            parent->children[pos] = asNode(node);
            parent->shiftEntries(pos + 2, -1);

            if constexpr (is_leaf)
            {
                if (is_sibling_left && (as(Leaf, sibling) == left_leaf))
                    left_leaf = as(Leaf, node);
                else if (!is_sibling_left && as(Leaf, sibling) == right_leaf)
                    right_leaf = as(Leaf, node);
            }
            --(parent->count);
        }
        else
        {
            // Do adoption.

            auto adopt_count = after_adopt - node->count;
            auto new_sep_sid = node->adopt(sibling, is_sibling_left, adopt_count, pos);

            parent->sids[std::min(pos, sibling_pos)] = new_sep_sid;
            parent->deltas[pos] = checkDelta(node->getDelta());
            parent->deltas[sibling_pos] = checkDelta(sibling->getDelta());
        }

        parent_updated = true;
    }
    else if (parent)
    {
        auto pos = parent->searchChild(asNode(node));
        auto delta = node->getDelta();
        parent_updated = parent->deltas[pos] != delta;
        parent->deltas[pos] = checkDelta(delta);
    }

    if (parent_updated)
        return parent;
    else
        return {};
}
