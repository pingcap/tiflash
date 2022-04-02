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

#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/TemporaryFile.h>
#include <common/logger_useful.h>

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

class MergeSortingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "MergeSorting";

public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    MergeSortingBlockInputStream(
        const BlockInputStreamPtr & input,
        SortDescription & description_,
        size_t max_merged_block_size_,
        size_t limit_,
        size_t max_bytes_before_external_sort_,
        const std::string & tmp_path_,
        const String & req_id);

    String getName() const override { return NAME; }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    SortDescription description;
    size_t max_merged_block_size;
    size_t limit;

    size_t max_bytes_before_external_sort;
    const std::string tmp_path;

    LoggerPtr log;

    Blocks blocks;
    size_t sum_bytes_in_blocks = 0;
    std::unique_ptr<IBlockInputStream> impl;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// (to avoid excessive virtual function calls and because constants cannot be serialized in Native format for temporary files)
    /// Save original block structure here.
    Block header;
    Block header_without_constants;

    /// Everything below is for external sorting.
    std::vector<std::unique_ptr<Poco::TemporaryFile>> temporary_files;

    /// For reading data from temporary file.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer<> compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path, const Block & header)
            : file_in(path)
            , compressed_in(file_in)
            , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, header, 0))
        {}
    };

    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

    BlockInputStreams inputs_to_merge;
};

} // namespace DB
