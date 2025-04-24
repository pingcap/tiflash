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

#include <Storages/DeltaMerge/File/DMFileMeta.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB::DM
{

class ReadBlockInfo;
using ReadBlockInfos = std::deque<ReadBlockInfo>;

class ReadBlockInfo
{
public:
    size_t start_pack_id = 0;
    size_t pack_count = 0;
    RSResult rs_result = RSResult::All;
    size_t read_rows = 0;

public:
    static ReadBlockInfos create(
        const RSResults & pack_res,
        const DMFileMeta::PackStats & pack_stats,
        size_t read_pack_limit,
        size_t rows_threshold_per_read);

    template <typename T>
    static ReadBlockInfos createWithRowIDs(
        std::span<T> row_ids,
        const std::vector<size_t> & pack_offset,
        const RSResults & pack_res,
        const DMFileMeta::PackStats & pack_stats,
        size_t rows_threshold_per_read);
};

} // namespace DB::DM
