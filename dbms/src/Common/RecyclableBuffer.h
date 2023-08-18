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

#include <cassert>
#include <memory>
#include <queue>

namespace DB
{
/// RecyclableBuffer recycles unused objects to avoid too much allocation of objects.
template <typename T>
class RecyclableBuffer
{
public:
    explicit RecyclableBuffer(size_t limit)
        : capacity(limit)
    {
        /// init empty objects
        for (size_t i = 0; i < limit; ++i)
        {
            empty_objects.push(std::make_shared<T>());
        }
    }
    bool hasEmpty() const
    {
        assert(!isOverflow(empty_objects));
        return !empty_objects.empty();
    }
    bool hasObjects() const
    {
        assert(!isOverflow(objects));
        return !objects.empty();
    }
    bool canPushEmpty() const
    {
        assert(!isOverflow(empty_objects));
        return !isFull(empty_objects);
    }
    bool canPush() const
    {
        assert(!isOverflow(objects));
        return !isFull(objects);
    }

    void popEmpty(std::shared_ptr<T> & t)
    {
        assert(!empty_objects.empty() && !isOverflow(empty_objects));
        t = empty_objects.front();
        empty_objects.pop();
    }
    void popObject(std::shared_ptr<T> & t)
    {
        assert(!objects.empty() && !isOverflow(objects));
        t = objects.front();
        objects.pop();
    }
    void pushObject(const std::shared_ptr<T> & t)
    {
        assert(!isFullOrOverflow(objects));
        objects.push(t);
    }
    void pushEmpty(const std::shared_ptr<T> & t)
    {
        assert(!isFullOrOverflow(empty_objects));
        empty_objects.push(t);
    }
    void pushEmpty(std::shared_ptr<T> && t)
    {
        assert(!isFullOrOverflow(empty_objects));
        empty_objects.push(std::move(t));
    }

private:
    bool isFullOrOverflow(const std::queue<std::shared_ptr<T>> & q) const { return q.size() >= capacity; }
    bool isOverflow(const std::queue<std::shared_ptr<T>> & q) const { return q.size() > capacity; }
    bool isFull(const std::queue<std::shared_ptr<T>> & q) const { return q.size() == capacity; }

    std::queue<std::shared_ptr<T>> empty_objects;
    std::queue<std::shared_ptr<T>> objects;
    size_t capacity;
};

} // namespace DB
