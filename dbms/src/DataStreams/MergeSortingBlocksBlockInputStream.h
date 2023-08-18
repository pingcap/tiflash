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

#include <Common/Logger.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <DataStreams/IProfilingBlockInputStream.h>

#include <queue>

namespace DB
{
/** Merges stream of sorted each-separately blocks to sorted as-a-whole stream of blocks.
  * If data to sort is too much, could use external sorting, with temporary files.
  */

/** Part of implementation. Merging array of ready (already read from somewhere) blocks.
  * Returns result of merge as stream of blocks, not more than 'max_merged_block_size' rows in each.
  */
class MergeSortingBlocksBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "MergeSortingBlocks";

public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingBlocksBlockInputStream(
        Blocks & blocks_,
        SortDescription & description_,
        const String & req_id,
        size_t max_merged_block_size_,
        size_t limit_ = 0);

    String getName() const override { return NAME; }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    Blocks & blocks;
    Block header;
    SortDescription description;
    size_t max_merged_block_size;
    size_t limit;
    size_t total_merged_rows = 0;

    using CursorImpls = std::vector<SortCursorImpl>;
    CursorImpls cursors;

    bool has_collation = false;

    std::priority_queue<SortCursor> queue;
    std::priority_queue<SortCursorWithCollation> queue_with_collation;

    /** Two different cursors are supported - with and without Collation.
     *  Templates are used (instead of virtual functions in SortCursor) for zero-overhead.
     */
    template <typename TSortCursor>
    Block mergeImpl(std::priority_queue<TSortCursor> & queue);

    LoggerPtr log;
};
} // namespace DB
