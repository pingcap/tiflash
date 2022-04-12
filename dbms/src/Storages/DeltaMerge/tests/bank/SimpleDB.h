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

#pragma once

#include <Core/Types.h>

#include <mutex>
#include <unordered_map>

namespace DB
{
namespace DM
{
namespace tests
{
class SimpleDB
{
public:
    void upsertRow(UInt64 id, UInt64 balance, UInt64 tso)
    {
        std::lock_guard guard{mutex};
        std::pair<UInt64, UInt64> value = std::make_pair(tso, balance);
        if (data.find(id) == data.end())
        {
            std::vector<std::pair<UInt64, UInt64>> values{value};
            data.emplace(id, values);
        }
        else
        {
            auto & all_value = data[id];
            all_value.push_back(value);
        }
    }

public:
    void insertBalance(UInt64 id, UInt64 balance, UInt64 tso) { upsertRow(id, balance, tso); }

    void updateBalance(UInt64 id, UInt64 balance, UInt64 tso) { upsertRow(id, balance, tso); }

    UInt64 selectBalance(UInt64 id, UInt64 tso)
    {
        std::lock_guard guard{mutex};
        UInt64 current_tso = 0;
        UInt64 result = 0;
        for (auto & p : data[id])
        {
            if (p.first <= tso && p.first >= current_tso)
            {
                current_tso = p.first;
                result = p.second;
            }
        }
        return result;
    }

    UInt64 sumBalance(UInt64 begin, UInt64 end, UInt64 tso)
    {
        UInt64 result = 0;
        for (UInt64 id = begin; id < end; id++)
        {
            result += selectBalance(id, tso);
        }
        return result;
    }

private:
    std::unordered_map<UInt64, std::vector<std::pair<UInt64, UInt64>>> data;
    std::mutex mutex;
};
} // namespace tests
} // namespace DM
} // namespace DB
