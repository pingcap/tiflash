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

#include <DataStreams/RuntimeFilter.h>

#include <unordered_map>
#include <vector>

namespace DB
{
class RuntimeFilter;

using RuntimeFilterPtr = std::shared_ptr<RuntimeFilter>;
using RuntimeFilteList = std::vector<RuntimeFilterPtr>;
extern RuntimeFilteList dummy_runtime_filter_list;

class RuntimeFilterMgr
{
public:
    RuntimeFilterMgr() = default;

    ~RuntimeFilterMgr() = default;

    RuntimeFilteList getLocalRuntimeFilterByIds(const std::vector<int> & ids);

    void registerRuntimeFilterList(std::vector<RuntimeFilterPtr> & rfList);

private:
    // Local rf id -> runtime filter ref
    std::unordered_map<int, RuntimeFilterPtr> local_runtime_filter_map;
};
} // namespace DB
