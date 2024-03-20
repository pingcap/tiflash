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

#pragma once

#include <Common/nocopyable.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <optional>
#include <utility>
#include <vector>
namespace DB::DM
{

template <typename IntervalType, typename ValueType>
struct Interval
{
    using interval_type = typename std::decay<IntervalType>::type;
    using value_type = typename std::decay<ValueType>::type;

    Interval(interval_type l, interval_type h, value_type v = {})
        : low(std::move(l))
        , high(std::move(h))
        , value(std::move(v))
    {}

    bool operator==(const Interval & other) const { return other.low == low && other.high == high; }

    interval_type low;
    interval_type high;
    value_type value;
};

// `IntervalTree` is a red-black tree based data structure that use to index intervals.
// Check `gtest_interval_tree.cpp` for its usage.
template <typename IntervalType, typename ValueType>
class IntervalTree
{
public:
    using Interval = Interval<IntervalType, ValueType>;
    using Intervals = std::vector<Interval>;
    using size_type = std::size_t;

    IntervalTree()
        : m_nill(new Node())
        , m_root(m_nill)
    {}

    ~IntervalTree()
    {
        destroySubtree(m_root);
        delete m_nill;
    }

    DISALLOW_COPY_AND_MOVE(IntervalTree);

    // Return true if insert successfully.
    // Return false if `interval` already exists.
    bool insert(Interval interval)
    {
        if (m_root == m_nill)
        {
            // Tree is empty
            assert(0 == m_size);
            m_root = new Node(std::move(interval), Color::Black, m_nill);
            m_size = 1;
            return true;
        }

        Node * node = findNode(m_root, interval);
        assert(node != m_nill);

        if (interval.low < node->intervals.front().low)
        {
            createChildNode(node, std::move(interval), Position::Left);
            return true;
        }

        if (node->intervals.front().low < interval.low)
        {
            createChildNode(node, std::move(interval), Position::Right);
            return true;
        }

        // node->intervals.front().low == interval.low

        if (!isNodeHasInterval(node, interval))
        {
            auto it = std::lower_bound(node->intervals.begin(), node->intervals.end(), interval, HighComparator());

            if (node->high < interval.high)
            {
                node->high = interval.high;
            }

            if (node->highest < node->high)
            {
                node->highest = node->high;
                insertionFixNodeLimits(node);
            }

            node->intervals.emplace(it, std::move(interval));
            ++m_size;

            return true;
        }

        // Value already exists
        return false;
    }

    // Return true if remove successfully.
    // Return false if `interval` not exists.
    bool remove(const Interval & interval)
    {
        if (m_root == m_nill)
        {
            // Tree is empty
            assert(0 == m_size);
            return false;
        }

        auto node = findNode(m_root, interval);
        assert(node != m_nill);

        auto it = std::find(node->intervals.begin(), node->intervals.end(), interval);
        if (it != node->intervals.cend())
        {
            node->intervals.erase(it);
            if (isNodeAboutToBeDestroyed(node))
            {
                auto child = m_nill;
                if (node->right == m_nill)
                {
                    child = node->left;
                }
                else if (node->left == m_nill)
                {
                    child = node->right;
                }
                else
                {
                    auto next_value_node = node->right;
                    while (next_value_node->left != m_nill)
                    {
                        next_value_node = next_value_node->left;
                    }
                    node->intervals = std::move(next_value_node->intervals);
                    node->high = std::move(next_value_node->high);
                    removeFixNodeLimits(node);
                    node = next_value_node;
                    child = next_value_node->right;
                }

                if (child == m_nill && node->parent == m_nill)
                {
                    // Node is root without children
                    swapRelations(node, child);
                    destroyNode(node);
                    return true;
                }

                if (Color::Red == node->color || Color::Red == child->color)
                {
                    swapRelations(node, child);
                    if (Color::Red == child->color)
                    {
                        child->color = Color::Black;
                    }
                }
                else
                {
                    assert(Color::Black == node->color);

                    if (child == m_nill)
                    {
                        child = node;
                    }
                    else
                    {
                        swapRelations(node, child);
                    }

                    removeFix(child);

                    if (node->parent != m_nill)
                    {
                        setChildNode(node->parent, m_nill, nodePosition(node));
                    }
                }

                if (node->parent != m_nill)
                {
                    removeFixNodeLimits(node->parent, 1);
                }
                destroyNode(node);
            }
            else
            {
                if (isEqual(interval.high, node->high))
                {
                    node->high = findHighest(node->intervals);
                }

                if (isEqual(interval.high, node->highest))
                {
                    removeFixNodeLimits(node);
                }

                --m_size;
            }

            return true;
        }

        // Value not found
        return false;
    }

    // Return value of `interval` if `interval` exists.
    // Return std::nullopt if `interval` not exists.
    std::optional<ValueType> find(const Interval & interval) const
    {
        if (m_root == m_nill)
        {
            // Tree is empty
            assert(0 == m_size);
            return std::nullopt;
        }

        auto node = findNode(m_root, interval);
        assert(node != m_nill);

        auto itr = std::find(node->intervals.cbegin(), node->intervals.cend(), interval);
        if (itr == node->intervals.cend())
        {
            return std::nullopt;
        }
        return itr->value;
    }

    // If `boundary` is true, two intervals are intersecting if `left1 <= right2 && left2 <= right1`,
    // equivalent to all intervals are closed.
    // If `boundary` is false, two intervals are intersecting if `left1 < right2 && left2 < right1`,
    // equivalent to all intervals are left-closed and right-open.
    Intervals findOverlappingIntervals(const Interval & interval, bool boundary) const
    {
        Intervals out;
        if (m_root != m_nill)
        {
            subtreeOverlappingIntervals(m_root, interval, boundary, Appender{out});
        }
        return out;
    }

    size_type size() const { return m_size; }

private:
    enum class Color : char
    {
        Black,
        Red
    };

    enum class Position : char
    {
        Left,
        Right
    };

    struct Appender final
    {
        template <typename Interval>
        void operator()(Interval && interval)
        {
            intervals.emplace_back(std::forward<Interval>(interval));
        }

        Intervals & intervals;
    };

    struct HighComparator final
    {
        template <typename T>
        bool operator()(const T & lhs, const T & rhs) const
        {
            return (lhs.high < rhs.high);
        }
    };

    struct Node
    {
        using interval_type = typename Interval::interval_type;

        Node() = default;

        Node(Interval interval, Color col, Node * nill)
            : color(col)
            , parent(nill)
            , left(nill)
            , right(nill)
            , high(interval.high)
            , lowest(interval.low)
            , highest(interval.high)
        {
            intervals.emplace_back(std::move(interval));
        }

        Color color = Color::Black;
        Node * parent = nullptr;
        Node * left = nullptr;
        Node * right = nullptr;

        interval_type high{};
        interval_type lowest{};
        interval_type highest{};
        Intervals intervals;
    };

    void destroySubtree(Node * node) const
    {
        assert(nullptr != node);

        if (node == m_nill)
        {
            return;
        }

        destroySubtree(node->left);
        destroySubtree(node->right);

        delete node;
    }

    Node * findNode(Node * node, const Interval & interval) const
    {
        assert(nullptr != node);
        assert(node != m_nill);

        auto child = m_nill;
        if (interval.low < node->intervals.front().low)
        {
            child = childNode(node, Position::Left);
        }
        else if (node->intervals.front().low < interval.low)
        {
            child = childNode(node, Position::Right);
        }
        else
        {
            return node;
        }

        return (child == m_nill) ? node : findNode(child, interval);
    }

    Node * siblingNode(Node * node) const
    {
        assert(nullptr != node);

        return (Position::Left == nodePosition(node)) ? childNode(node->parent, Position::Right)
                                                      : childNode(node->parent, Position::Left);
    }

    Node * childNode(Node * node, Position position) const
    {
        assert(nullptr != node);

        switch (position)
        {
        case Position::Left:
            return node->left;
        case Position::Right:
            return node->right;
        default:
            assert(false);
            return nullptr;
        }
    }

    void setChildNode(Node * node, Node * child, Position position) const
    {
        assert(nullptr != node && nullptr != child);
        assert(node != m_nill);

        switch (position)
        {
        case Position::Left:
            node->left = child;
            break;
        case Position::Right:
            node->right = child;
            break;
        default:
            assert(false);
            break;
        }

        if (child != m_nill)
        {
            child->parent = node;
        }
    }

    Position nodePosition(Node * node) const
    {
        assert(nullptr != node && nullptr != node->parent);

        return (node->parent->left == node) ? Position::Left : Position::Right;
    }

    void createChildNode(Node * parent, Interval interval, Position position)
    {
        assert(nullptr != parent);
        assert(childNode(parent, position) == m_nill);

        auto child = new Node(std::move(interval), Color::Red, m_nill);
        setChildNode(parent, child, position);
        insertionFixNodeLimits(child);
        insertionFix(child);
        ++m_size;
    }

    void destroyNode(Node * node)
    {
        --m_size;
        delete node;
    }

    void updateNodeLimits(Node * node) const
    {
        assert(nullptr != node);

        auto left = isNodeAboutToBeDestroyed(node->left) ? m_nill : node->left;
        auto right = isNodeAboutToBeDestroyed(node->right) ? m_nill : node->right;

        const auto & lowest = (left != m_nill) ? left->lowest : node->intervals.front().low;

        if (isNotEqual(node->lowest, lowest))
        {
            node->lowest = lowest;
        }

        const auto & highest = std::max({left->highest, right->highest, node->high});

        if (isNotEqual(node->highest, highest))
        {
            node->highest = highest;
        }
    }

    bool isNodeAboutToBeDestroyed(Node * node) const
    {
        assert(nullptr != node);

        return (node != m_nill && node->intervals.empty());
    }

    template <typename Callback>
    void subtreeOverlappingIntervals(Node * node, const Interval & interval, bool boundary, Callback && callback) const
    {
        assert(nullptr != node);

        if (node == m_nill)
        {
            return;
        }

        if (node->left != m_nill
            && (boundary ? !(node->left->highest < interval.low) : interval.low < node->left->highest))
        {
            subtreeOverlappingIntervals(node->left, interval, boundary, callback);
        }

        if (boundary ? !(interval.high < node->intervals.front().low) : node->intervals.front().low < interval.high)
        {
            for (auto it = node->intervals.rbegin(); it != node->intervals.rend(); ++it)
            {
                if (boundary ? !(it->high < interval.low) : interval.low < it->high)
                {
                    callback(*it);
                }
                else
                {
                    break;
                }
            }

            subtreeOverlappingIntervals(node->right, interval, boundary, std::forward<Callback>(callback));
        }
    }

    template <typename Callback>
    void subtreeIntervalsContainPoint(Node * node, const IntervalType & point, bool boundary, Callback && callback)
        const
    {
        assert(nullptr != node);

        if (node == m_nill)
        {
            return;
        }

        if (node->left != m_nill && (boundary ? !(node->left->highest < point) : point < node->left->highest))
        {
            subtreeIntervalsContainPoint(node->left, point, boundary, callback);
        }

        if (boundary ? !(point < node->intervals.front().low) : node->intervals.front().low < point)
        {
            for (auto it = node->intervals.rbegin(); it != node->intervals.rend(); ++it)
            {
                if (boundary ? !(it->high < point) : point < it->high)
                {
                    callback(*it);
                }
                else
                {
                    break;
                }
            }

            subtreeIntervalsContainPoint(node->right, point, boundary, std::forward<Callback>(callback));
        }
    }

    bool isNodeHasInterval(Node * node, const Interval & interval) const
    {
        assert(nullptr != node);

        return (node->intervals.cend() != std::find(node->intervals.cbegin(), node->intervals.cend(), interval));
    }

    IntervalType findHighest(const Intervals & intervals) const
    {
        assert(!intervals.empty());

        auto it = std::max_element(intervals.cbegin(), intervals.cend(), HighComparator());

        assert(it != intervals.cend());

        return it->high;
    }

    bool isNotEqual(const IntervalType & lhs, const IntervalType & rhs) const { return (lhs < rhs || rhs < lhs); }

    bool isEqual(const IntervalType & lhs, const IntervalType & rhs) const { return !isNotEqual(lhs, rhs); }

    void swapRelations(Node * node, Node * child)
    {
        assert(nullptr != node && nullptr != child);

        if (node->parent == m_nill)
        {
            if (child != m_nill)
            {
                child->parent = m_nill;
            }
            m_root = child;
        }
        else
        {
            setChildNode(node->parent, child, nodePosition(node));
        }
    }

    void rotateCommon(Node * node, Node * child) const
    {
        assert(nullptr != node && nullptr != child);
        assert(node != m_nill && child != m_nill);

        std::swap(node->color, child->color);

        updateNodeLimits(node);

        if (child->highest < node->highest)
        {
            child->highest = node->highest;
        }

        if (node->lowest < child->lowest)
        {
            child->lowest = node->lowest;
        }
    }

    Node * rotateLeft(Node * node)
    {
        assert(nullptr != node && nullptr != node->right);

        auto child = node->right;
        swapRelations(node, child);
        setChildNode(node, child->left, Position::Right);
        setChildNode(child, node, Position::Left);
        rotateCommon(node, child);

        return child;
    }

    Node * rotateRight(Node * node)
    {
        assert(nullptr != node && nullptr != node->left);

        auto child = node->left;
        swapRelations(node, child);
        setChildNode(node, child->right, Position::Left);
        setChildNode(child, node, Position::Right);
        rotateCommon(node, child);

        return child;
    }

    Node * rotate(Node * node)
    {
        assert(nullptr != node);

        switch (nodePosition(node))
        {
        case Position::Left:
            return rotateRight(node->parent);
        case Position::Right:
            return rotateLeft(node->parent);
        default:
            assert(false);
            return nullptr;
        }
    }

    void insertionFixNodeLimits(Node * node)
    {
        assert(nullptr != node && nullptr != node->parent);

        while (node->parent != m_nill)
        {
            auto finish = true;

            if (node->parent->highest < node->highest)
            {
                node->parent->highest = node->highest;
                finish = false;
            }

            if (node->lowest < node->parent->lowest)
            {
                node->parent->lowest = node->lowest;
                finish = false;
            }

            if (finish)
            {
                break;
            }

            node = node->parent;
        }
    }

    void removeFixNodeLimits(Node * node, size_type minRange = 0)
    {
        assert(nullptr != node && nullptr != node->parent);

        size_type range = 0;
        while (node != m_nill)
        {
            bool finish = (minRange < range);

            updateNodeLimits(node);

            if (isNotEqual(node->highest, node->parent->highest))
            {
                finish = false;
            }

            if (isNotEqual(node->lowest, node->parent->lowest))
            {
                finish = false;
            }

            if (finish)
            {
                break;
            }

            node = node->parent;
            ++range;
        }
    }

    void insertionFix(Node * node)
    {
        assert(nullptr != node && nullptr != node->parent);

        while (Color::Red == node->color && Color::Red == node->parent->color)
        {
            auto parent = node->parent;
            auto uncle = siblingNode(parent);
            switch (uncle->color)
            {
            case Color::Red:
                uncle->color = Color::Black;
                parent->color = Color::Black;
                parent->parent->color = Color::Red;
                node = parent->parent;
                break;
            case Color::Black:
                if (nodePosition(node) != nodePosition(parent))
                {
                    parent = rotate(node);
                }
                node = rotate(parent);
                break;
            default:
                assert(false);
                break;
            }
        }

        if (node->parent == m_nill && Color::Black != node->color)
        {
            node->color = Color::Black;
        }
    }

    void removeFix(Node * node)
    {
        assert(nullptr != node && nullptr != node->parent);

        while (Color::Black == node->color && node->parent != m_nill)
        {
            auto sibling = siblingNode(node);
            if (Color::Red == sibling->color)
            {
                rotate(sibling);
                sibling = siblingNode(node);
            }

            assert(nullptr != sibling && nullptr != sibling->left && nullptr != sibling->right);
            assert(Color::Black == sibling->color);

            if (Color::Black == sibling->left->color && Color::Black == sibling->right->color)
            {
                sibling->color = Color::Red;
                node = node->parent;
            }
            else
            {
                if (Position::Left == nodePosition(sibling) && Color::Black == sibling->left->color)
                {
                    sibling = rotateLeft(sibling);
                }
                else if (Position::Right == nodePosition(sibling) && Color::Black == sibling->right->color)
                {
                    sibling = rotateRight(sibling);
                }
                rotate(sibling);
                node = siblingNode(node->parent);
            }
        }

        if (Color::Black != node->color)
        {
            node->color = Color::Black;
        }
    }

    Node * m_nill;
    Node * m_root;
    size_type m_size{0};
};
} // namespace DB::DM
