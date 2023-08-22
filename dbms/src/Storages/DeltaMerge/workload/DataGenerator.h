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
#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB::DM::tests
{
struct WorkloadOptions;
struct TableInfo;
class KeyGenerator;
class TimestampGenerator;

class DataGenerator
{
public:
    static std::unique_ptr<DataGenerator> create(
        const WorkloadOptions & opts,
        const TableInfo & table_info,
        TimestampGenerator & ts_gen);
    virtual std::tuple<Block, uint64_t> get(uint64_t key) = 0;
    virtual ~DataGenerator() = default;
};

std::string blockToString(const Block & block);

} // namespace DB::DM::tests