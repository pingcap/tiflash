// Copyright 2023 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/Page/Page.h>

namespace DB::DM::Remote
{

class RemotePageInputStream
{
public:
    RemotePageInputStream(SegmentReadTaskPtr seg_task_, size_t expected_block_size_)
        : seg_task(seg_task_)
        , expected_block_size(expected_block_size_)
    {}

    Page readCFTinyPage(PageIdU64 page_id);

    // Read a block from the segment's mem-table. The return block respect
    // `rows_limit`.
    // You should call this function multiple times to read out all blocks
    // in mem-tables, untils it return an empty block.
    Block readMemTableBlock();

private:
    SegmentReadTaskPtr seg_task;
    const size_t expected_block_size;
};

} // namespace DB::DM::Remote
