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

#include <Common/ArenaWithFreeLists.h>

namespace DB
{
/// A simple wrap around ArenaWithFreeLists.
class RecycledAllocator : public boost::noncopyable
{
public:
    explicit RecycledAllocator(const size_t initial_size = 65536, const size_t growth_factor = 2)
        : arena(initial_size, growth_factor)
    {}

    char * alloc(size_t size) { return arena.alloc(size); }

    void free(char * buf, const size_t size) { arena.free(buf, size); }

    std::shared_ptr<char> createMemoryReference(char * addr, size_t size)
    {
        return std::shared_ptr<char>(addr, [=, this](char * a) { arena.free(a, size); });
    }

private:
    ArenaWithFreeLists arena;
};

} // namespace DB
