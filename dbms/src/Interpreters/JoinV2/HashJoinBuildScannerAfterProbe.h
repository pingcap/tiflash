// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/JoinV2/HashJoinProbe.h>

namespace DB
{

class HashJoin;
class JoinBuildScannerAfterProbe
{
public:
    explicit JoinBuildScannerAfterProbe(HashJoin * join);

    Block scan(JoinProbeWorkerData & wd);

private:
    template <typename KeyGetter, ASTTableJoin::Kind kind, bool need_row_data, bool need_other_block_data>
    Block scanImpl(JoinProbeWorkerData & wd);

    template <bool need_row_data, bool need_other_block_data>
    void ALWAYS_INLINE
    insertRowToBatch(JoinProbeWorkerData & wd, RowPtr row_ptr, size_t index, size_t insert_batch_max_size) const
    {
        if constexpr (need_row_data)
        {
            wd.insert_batch.push_back(row_ptr);
            if unlikely (wd.insert_batch.size() >= insert_batch_max_size)
                flushInsertBatch<false>(wd);
        }
        if constexpr (need_other_block_data)
        {
            wd.selective_offsets.push_back(index);
        }
    }

    template <bool last_flush>
    void flushInsertBatch(JoinProbeWorkerData & wd) const;

    void fillNullMapWithZero(JoinProbeWorkerData & wd) const;

private:
    using FuncType = Block (JoinBuildScannerAfterProbe::*)(JoinProbeWorkerData &);
    FuncType scan_func_ptr = nullptr;

    HashJoin * join;
    std::mutex scan_build_lock;
    size_t scan_build_index = 0;
    /// Used for deserializing join key and getting required key offset
    std::unique_ptr<void, std::function<void(void *)>> join_key_getter;
    Columns materialized_key_columns;
};


} // namespace DB
