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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/SegmentReadResultChannel.h>

namespace DB::DM
{

SegmentReadResultChannel::SegmentReadResultChannel(const Options & options)
    : expected_sources(options.expected_sources)
    , debug_tag(options.debug_tag)
    , max_pending_blocks(options.max_pending_blocks)
    , header(options.header)
    , on_first_read(options.on_first_read)
    , log(Logger::get(options.debug_tag))
{
    LOG_DEBUG(log, "Created ResultChannel, expected_sources={}", expected_sources);
}

SegmentReadResultChannel::~SegmentReadResultChannel()
{
    auto [pop_times, pop_empty_times, max_queue_size] = q.getStat();
    auto pop_empty_ratio = pop_times > 0 ? pop_empty_times * 1.0 / pop_times : 0.0;
    auto total_count = blk_stat.totalCount();
    auto total_bytes = blk_stat.totalBytes();
    auto blk_avg_bytes = total_count > 0 ? total_bytes / total_count : 0;
    auto approximate_max_pending_block_bytes = blk_avg_bytes * max_queue_size;

    LOG_DEBUG(
        log,
        "Destroy ResultChannel, pop={} pop_empty={} pop_empty_ratio={} max_queue_size={} blk_avg_bytes={} approximate_max_pending_block_bytes={:.2f}MB total_count={} total_bytes={:.2f}MB",
        pop_times,
        pop_empty_times,
        pop_empty_ratio,
        max_queue_size,
        blk_avg_bytes,
        approximate_max_pending_block_bytes / 1024.0 / 1024.0,
        total_count,
        total_bytes / 1024.0 / 1024.0);
}

void SegmentReadResultChannel::pushBlock(Block && block)
{
    // Just a simple check to verify the block structure.
    RUNTIME_CHECK(
        block.columns() == header.columns(),
        block.columns(),
        header.columns());

    blk_stat.push(block);
    global_blk_stat.push(block);
    q.push(std::move(block), nullptr);
}

void SegmentReadResultChannel::finish(const String debug_source_tag)
{
    std::unique_lock lock(mu);

    if (is_finished)
        return;

    if (finished_sources.contains(debug_source_tag))
        RUNTIME_CHECK_MSG(false, "ResultChannel {} source {} is already finished", debug_tag, debug_source_tag);

    finished_sources.emplace(debug_source_tag);
    RUNTIME_CHECK(
        finished_sources.size() <= expected_sources,
        finished_sources,
        expected_sources);

    LOG_DEBUG(
        log,
        "ResultChannel source finished: {}, finished_sources={} expected_sources={}",
        debug_source_tag,
        finished_sources.size(),
        expected_sources);

    if (finished_sources.size() == expected_sources)
    {
        is_finished = true;
        q.finish();
    }
}

void SegmentReadResultChannel::finishWithError(const DB::Exception & e)
{
    LOG_DEBUG(log, "ResultChannel finish with error: {}", e.message());

    std::unique_lock lock(mu);
    if (!has_error)
    {
        exception = e;
        has_error = true;
    }
    if (!is_finished)
    {
        is_finished = true;
        q.finish();
    }
}

void SegmentReadResultChannel::popBlock(Block & block)
{
    triggerFirstRead();

    // Note: Actually this implementation is currently thread-safe. However
    // we don't provide thread-safe guarantee to the caller, to make our life
    // easier.
    q.pop(block);
    blk_stat.pop(block);
    global_blk_stat.pop(block);
    if (has_error)
        throw exception;
}

bool SegmentReadResultChannel::tryPopBlock(Block & block)
{
    triggerFirstRead();

    if (!q.tryPop(block))
        return false;

    blk_stat.pop(block);
    global_blk_stat.pop(block);
    if (has_error)
        throw exception;
    return true;
}

UInt64 SegmentReadResultChannel::refConsumer()
{
    RUNTIME_CHECK(alive_consumers >= 0, alive_consumers);
    return static_cast<UInt64>(alive_consumers.fetch_add(1));
}

UInt64 SegmentReadResultChannel::derefConsumer()
{
    auto c = alive_consumers.fetch_sub(1);
    RUNTIME_CHECK(c >= 0, alive_consumers);
    return static_cast<UInt64>(c);
}

bool SegmentReadResultChannel::valid() const
{
    return !has_error && !is_finished && alive_consumers > 0;
}

bool SegmentReadResultChannel::hasAliveConsumers() const
{
    return alive_consumers > 0;
}

void SegmentReadResultChannel::triggerFirstRead()
{
    if (!on_first_read)
        return;

    std::call_once(has_read_once, [this]() {
        SegmentReadResultChannelPtr this_ptr;
        try
        {
            this_ptr = shared_from_this();
        }
        catch (std::bad_weak_ptr & e)
        {
            RUNTIME_CHECK_MSG(false, "Must be called from SegmentReadResultChannelPtr");
        }
        on_first_read(this_ptr);
    });
}

bool SegmentReadResultChannel::isFull() const
{
    return blk_stat.pendingCount() >= static_cast<Int64>(max_pending_blocks);
}

} // namespace DB::DM
