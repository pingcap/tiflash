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

#include <common/types.h>

#include <mutex>
#include <unordered_set>

namespace DB
{
class FIFOQueryIdCache
{
public:
    bool contains(const String & id) const
    {
        assert(set.size() == fifo.size());
        return !id.empty() && set.contains(id);
    }

    bool add(const String & id)
    {
        assert(set.size() == fifo.size());
        if (id.empty() || set.contains(id))
            return false;
        if (set.size() >= capacity)
        {
            auto evicted_id = fifo.back();
            fifo.pop_back();
            set.erase(evicted_id);
        }
        fifo.push_front(id);
        set.insert(id);
        return true;
    }

private:
    std::deque<String> fifo;
    std::unordered_set<String> set;
    size_t capacity{10000};
};
} // namespace DB
