#pragma once

#include <common/defines.h>

#include <boost/noncopyable.hpp>

namespace DB
{
template <typename Node>
struct SimpleIntrusiveNode : private boost::noncopyable
{
    Node * next;
    Node * prev;

    SimpleIntrusiveNode()
    {
        next = getThis();
        prev = getThis();
    }

    ~SimpleIntrusiveNode() = default;

    /// Attach self to the next of head. Do nothing when call on a single node.
    void appendTo(Node * head)
    {
        next = head->next;
        prev = head;
        head->next->prev = getThis();
        head->next = getThis();
    }

    /// Attach self to the prev of head. Do nothing when call on a single node.
    void prependTo(Node * head)
    {
        prev = head->prev;
        next = head;
        head->prev->next = getThis();
        head->prev = getThis();
    }

    /// Detach self from a list and be a single node. Do nothing when call on a single node.
    void detach()
    {
        prev->next = next;
        next->prev = prev;
        next = getThis();
        prev = getThis();
    }

    bool isSingle() const
    {
        return next == getThis();
    }

protected:
    ALWAYS_INLINE Node * getThis()
    {
        return static_cast<Node *>(this);
    }

    ALWAYS_INLINE const Node * getThis() const
    {
        return static_cast<const Node *>(this);
    }
};

} // namespace DB
