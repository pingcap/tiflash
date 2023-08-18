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

#include <Common/Allocator.h>
#include <Common/Arena.h>


namespace DB
{
/// Allocator which proxies all allocations to Arena. Used in aggregate functions.
class ArenaAllocator
{
public:
    static void * alloc(size_t size, Arena * arena) { return arena->alloc(size); }

    static void * realloc(void * buf, size_t old_size, size_t new_size, Arena * arena)
    {
        char const * data = reinterpret_cast<char *>(buf);

        // Invariant should be maintained: new_size > old_size
        if (data + old_size == arena->head->pos)
        {
            // Consecutive optimization
            arena->allocContinue(new_size - old_size, data);
            return reinterpret_cast<void *>(const_cast<char *>(data));
        }
        else
        {
            return arena->realloc(data, old_size, new_size);
        }
    }

    static void free(void * /*buf*/, size_t /*size*/)
    {
        // Do nothing, trash in arena remains.
    }
};


/// Switches to ordinary Allocator after REAL_ALLOCATION_TRESHOLD bytes to avoid fragmentation and trash in Arena.
template <
    size_t REAL_ALLOCATION_TRESHOLD = 4096,
    typename TRealAllocator = Allocator<false>,
    typename TArenaAllocator = ArenaAllocator>
class MixedArenaAllocator : private TRealAllocator
{
public:
    void * alloc(size_t size, Arena * arena)
    {
        return (size < REAL_ALLOCATION_TRESHOLD) ? TArenaAllocator::alloc(size, arena) : TRealAllocator::alloc(size);
    }

    void * realloc(void * buf, size_t old_size, size_t new_size, Arena * arena)
    {
        // Invariant should be maintained: new_size > old_size

        if (new_size < REAL_ALLOCATION_TRESHOLD)
            return TArenaAllocator::realloc(buf, old_size, new_size, arena);

        if (old_size >= REAL_ALLOCATION_TRESHOLD)
            return TRealAllocator::realloc(buf, old_size, new_size);

        void * new_buf = TRealAllocator::alloc(new_size);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

    void free(void * buf, size_t size)
    {
        if (size >= REAL_ALLOCATION_TRESHOLD)
            TRealAllocator::free(buf, size);
    }
};


template <size_t N = 64, typename Base = ArenaAllocator>
class ArenaAllocatorWithStackMemoty : public Base
{
    char stack_memory[N];

public:
    void * alloc(size_t size, Arena * arena) { return (size > N) ? Base::alloc(size, arena) : stack_memory; }

    void * realloc(void * buf, size_t old_size, size_t new_size, Arena * arena)
    {
        /// Was in stack_memory, will remain there.
        if (new_size <= N)
            return buf;

        /// Already was big enough to not fit in stack_memory.
        if (old_size > N)
            return Base::realloc(buf, old_size, new_size, arena);

        /// Was in stack memory, but now will not fit there.
        void * new_buf = Base::alloc(new_size, arena);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

    void free(void * /*buf*/, size_t /*size*/) {}
};

} // namespace DB
