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

#include <Storages/DeltaMerge/workload/TableGenerator.h>

#include <random>

namespace DB::DM::tests
{
class ReadColumnsGenerator
{
public:
    static std::unique_ptr<ReadColumnsGenerator> create(const TableInfo & table_info)
    {
        return std::make_unique<ReadColumnsGenerator>(table_info);
    }

    explicit ReadColumnsGenerator(const TableInfo & table_info_)
        : table_info(table_info_)
        , rand_gen(std::random_device()())
        , uniform_dist(0, table_info_.columns->size() - 1)
    {}

    ColumnDefines readColumns()
    {
        ColumnDefines cols;
        int cnt = uniform_dist(rand_gen) + 1;
        for (int i = 0; i < cnt; i++)
        {
            auto & col = (*table_info.columns)[uniform_dist(rand_gen)];
            auto itr = std::find_if(cols.begin(), cols.end(), [&col](const auto & a) { return a.id == col.id; });
            if (itr != cols.end())
            {
                continue;
            }
            cols.push_back(col);
        }
        return cols;
    }

    int streamCount() { return rand_gen() % max_stream_count + 1; }

private:
    const TableInfo & table_info;
    std::mt19937 rand_gen;
    std::uniform_int_distribution<int> uniform_dist;

    static constexpr int max_stream_count = 8;
};
} // namespace DB::DM::tests