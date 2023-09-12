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

#include <Storages/KVStore/Decode/TableRowIDMinMax.h>

namespace DB
{
std::unordered_map<TableID, TableRowIDMinMax> TableRowIDMinMax::data;
std::mutex TableRowIDMinMax::mutex;

const TableRowIDMinMax & TableRowIDMinMax::getMinMax(const TableID table_id)
{
    std::lock_guard lock(mutex);

    if (auto it = data.find(table_id); it != data.end())
        return it->second;
    return data.try_emplace(table_id, table_id).first->second;
}

} // namespace DB
