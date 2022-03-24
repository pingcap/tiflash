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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/dedupUtils.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <thread>


namespace DB
{
class DedupSortedBlockInputStream : public IProfilingBlockInputStream
{
public:
    static BlockInputStreams createStreams(BlockInputStreams & inputs, const SortDescription & description);

    DedupSortedBlockInputStream(BlockInputStreams & inputs, const SortDescription & description);

    ~DedupSortedBlockInputStream();

    Block readImpl() override;

    Block getHeader() const override
    {
        return children[0]->getHeader();
    }

    String getName() const override
    {
        return "DedupSorted";
    }

    bool isGroupedOutput() const override
    {
        return true;
    }

    bool isSortedOutput() const override
    {
        return false;
    }

    const SortDescription & getSortDescription() const override
    {
        return description;
    }

private:
    void asyncDedupByQueue();
    void asynFetch(size_t position);

    void fetchBlock(size_t pisition);

    void readFromSource(DedupCursors & output, BoundQueue & bounds);

    void pushBlockBounds(const DedupingBlockPtr & block, BoundQueue & bounds);

    bool outputAndUpdateCursor(DedupCursors & cursors, BoundQueue & bounds, DedupCursor & cursor);

private:
    Poco::Logger * log;
    BlockInputStreams children;
    const SortDescription description;

    const size_t queue_max;

    BlocksFifoPtrs source_blocks;
    BlocksFifo output_block;

    std::unique_ptr<std::thread> dedup_thread;

    ThreadPool readers;

    size_t finished_streams = 0;
};

} // namespace DB
