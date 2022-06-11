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

// modified from https://github.com/microsoft/snmalloc/blob/0.6.0/src/snmalloc/ds/mpmcstack.h
// MIT License: https://github.com/microsoft/snmalloc/blob/0.6.0/LICENSE
#pragma once
#include <common/aba.h>

namespace common
{
static constexpr size_t MPMC_STACK_CACHELINE_SIZE = 64;

template <class T>
class MPMCStack
{
    using ABAType = ABA<T>;

private:
    alignas(MPMC_STACK_CACHELINE_SIZE) ABAType stack;

    static T * racyRead(std::atomic<T *> & ptr)
    {
        return ptr.load(std::memory_order_relaxed);
    }

public:
    explicit MPMCStack(bool force_generic = false)
    {
        stack.init(nullptr, force_generic);
    };

    void push(T * item)
    {
        static_assert(
            std::is_same<decltype(T::next), std::atomic<T *>>::value,
            "T->next must be an std::atomic<T*>");

        return push(item, item);
    }

    void push(T * first, T * last)
    {
        // Pushes an item on the stack.
        auto cmp = stack.read();

        do
        {
            auto top = cmp.ptr();
            last->next.store(top, std::memory_order_release);
        } while (!cmp.storeConditional(first));
    }

    T * pop()
    {
        // Returns the next item. If the returned value is decommitted, it is
        // possible for the read of top->next to segfault.
        auto cmp = stack.read();
        T * top;
        T * next;

        do
        {
            top = cmp.ptr();

            if (top == nullptr)
                break;

            // The following read can race with non-atomic accesses
            // this is undefined behaviour. There is no way to use
            // CAS sensibly that conforms to the standard with optimistic
            // concurrency.
            next = racyRead(top->next);
        } while (!cmp.storeConditional(next));

        return top;
    }

    T * popAll()
    {
        // Returns all items as a linked list, leaving an empty stack.
        auto cmp = stack.read();
        T * top;

        do
        {
            top = cmp.ptr();

            if (top == nullptr)
                break;
        } while (!cmp.storeConditional(nullptr));

        return top;
    }
};
} // namespace common