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

#include <common/defines.h>

#include <boost/noncopyable.hpp>
#include <cassert>

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

    /// Attach self to the next of head.
    void appendTo(Node * head)
    {
        assert(isSingle());

        next = head->next;
        prev = head;
        head->next->prev = getThis();
        head->next = getThis();
    }

    /// Attach self to the prev of head.
    void prependTo(Node * head)
    {
        assert(isSingle());

        prev = head->prev;
        next = head;
        head->prev->next = getThis();
        head->prev = getThis();
    }

    /// Detach self from a list and be a single node.
    void detach()
    {
        prev->next = next;
        next->prev = prev;
        next = getThis();
        prev = getThis();
    }

    bool isSingle() const { return next == getThis(); }

protected:
    ALWAYS_INLINE Node * getThis() { return static_cast<Node *>(this); }

    ALWAYS_INLINE const Node * getThis() const { return static_cast<const Node *>(this); }
};

} // namespace DB
