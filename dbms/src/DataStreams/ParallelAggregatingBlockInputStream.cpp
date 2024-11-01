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

#include <Common/FmtUtils.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/MergingAndConvertingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>

namespace DB
{
ParallelAggregatingBlockInputStream::ParallelAggregatingBlockInputStream(
    const BlockInputStreams & inputs,
    const BlockInputStreams & additional_inputs_at_end,
    const Aggregator::Params & params_,
    bool final_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    const String & req_id)
    : log(Logger::get(req_id))
    , params(params_)
    , aggregator(params, req_id)
    , final(final_)
    , max_threads(std::min(inputs.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , keys_size(params.keys_size)
    , aggregates_size(params.aggregates_size)
    , handler(*this)
    , processor(inputs, additional_inputs_at_end, max_threads, handler, log)
{
    children = inputs;
    children.insert(children.end(), additional_inputs_at_end.begin(), additional_inputs_at_end.end());
}


Block ParallelAggregatingBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


void ParallelAggregatingBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    if (!executed)
        processor.cancel(kill);
}


Block ParallelAggregatingBlockInputStream::readImpl()
{
    if (!executed)
    {
        CancellationHook hook = [&]() {
            return this->isCancelled();
        };
        aggregator.setCancellationHook(hook);

        execute();

        if (isCancelledOrThrowIfKilled())
            return {};

        if (!aggregator.hasSpilledData())
        {
            /** If all partially-aggregated data is in RAM, then merge them in parallel, also in RAM.
                */
            auto merging_buckets = aggregator.mergeAndConvertToBlocks(many_data, final, max_threads);
            if (!merging_buckets)
            {
                impl = std::make_unique<NullBlockInputStream>(aggregator.getHeader(final));
            }
            else
            {
                RUNTIME_CHECK(merging_buckets->getConcurrency() > 0);
                if (merging_buckets->getConcurrency() > 1)
                {
                    BlockInputStreams merging_streams;
                    for (size_t i = 0; i < merging_buckets->getConcurrency(); ++i)
                        merging_streams.push_back(std::make_shared<MergingAndConvertingBlockInputStream>(merging_buckets, i, log->identifier()));
                    impl = std::make_unique<UnionBlockInputStream<>>(merging_streams, BlockInputStreams{}, max_threads, log->identifier());
                }
                else
                {
                    impl = std::make_unique<MergingAndConvertingBlockInputStream>(merging_buckets, 0, log->identifier());
                }
            }
        }
        else
        {
            /** If there are temporary files with partially-aggregated data on the disk,
                *  then read and merge them, spending the minimum amount of memory.
                */

            aggregator.finishSpill();
            LOG_INFO(log, "Begin restore data from disk for aggregation.");
            BlockInputStreams input_streams = aggregator.restoreSpilledData();
            impl = std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(
                input_streams,
                params,
                final,
                temporary_data_merge_threads,
                temporary_data_merge_threads,
                log->identifier());
        }

        executed = true;
    }

    if (isCancelledOrThrowIfKilled() || !impl)
        return {};

    return impl->read();
}

void ParallelAggregatingBlockInputStream::Handler::onBlock(Block & block, size_t thread_num)
{
    parent.aggregator.executeOnBlock(
        block,
        *parent.many_data[thread_num],
        parent.threads_data[thread_num].key_columns,
        parent.threads_data[thread_num].aggregate_columns);

    parent.threads_data[thread_num].src_rows += block.rows();
    parent.threads_data[thread_num].src_bytes += block.bytes();
}

void ParallelAggregatingBlockInputStream::Handler::onFinishThread(size_t thread_num)
{
    if (!parent.isCancelled() && parent.aggregator.hasSpilledData())
    {
        /// Flush data in the RAM to disk. So it's easier to unite them later.
        auto & data = *parent.many_data[thread_num];

        if (data.isConvertibleToTwoLevel())
            data.convertToTwoLevel();

        if (!data.empty())
            parent.aggregator.spill(data);
    }
}

void ParallelAggregatingBlockInputStream::Handler::onFinish()
{
    if (!parent.isCancelled() && parent.aggregator.hasSpilledData())
    {
        /// It may happen that some data has not yet been flushed,
        ///  because at the time of `onFinishThread` call, no data has been flushed to disk, and then some were.
        for (auto & data : parent.many_data)
        {
            if (data->isConvertibleToTwoLevel())
                data->convertToTwoLevel();

            if (!data->empty())
                parent.aggregator.spill(*data);
        }
    }
}

void ParallelAggregatingBlockInputStream::Handler::onException(std::exception_ptr & exception, size_t thread_num)
{
    parent.exceptions[thread_num] = exception;
    Int32 old_value = -1;
    parent.first_exception_index.compare_exchange_strong(old_value, static_cast<Int32>(thread_num), std::memory_order_seq_cst, std::memory_order_relaxed);

    if (!parent.executed)
        /// use cancel instead of kill to avoid too many useless error message
        parent.cancel(false);
}


void ParallelAggregatingBlockInputStream::execute()
{
    many_data.resize(max_threads);
    exceptions.resize(max_threads);

    for (size_t i = 0; i < max_threads; ++i)
        threads_data.emplace_back(keys_size, aggregates_size);
    aggregator.initThresholdByAggregatedDataVariantsSize(many_data.size());

    LOG_TRACE(log, "Aggregating");

    Stopwatch watch;

    for (auto & elem : many_data)
        elem = std::make_shared<AggregatedDataVariants>();

    processor.process();
    processor.wait();

    if (first_exception_index != -1)
        std::rethrow_exception(exceptions[first_exception_index]);

    if (isCancelledOrThrowIfKilled())
        return;

    double elapsed_seconds = watch.elapsedSeconds();

    size_t total_src_rows = 0;
    size_t total_src_bytes = 0;
    for (size_t i = 0; i < max_threads; ++i)
    {
        size_t rows = many_data[i]->size();
        LOG_TRACE(
            log,
            "Aggregated. {} to {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
            threads_data[i].src_rows,
            rows,
            (threads_data[i].src_bytes / 1048576.0),
            elapsed_seconds,
            threads_data[i].src_rows / elapsed_seconds,
            threads_data[i].src_bytes / elapsed_seconds / 1048576.0);

        total_src_rows += threads_data[i].src_rows;
        total_src_bytes += threads_data[i].src_bytes;
    }
    LOG_TRACE(
        log,
        "Total aggregated. {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
        total_src_rows,
        (total_src_bytes / 1048576.0),
        elapsed_seconds,
        total_src_rows / elapsed_seconds,
        total_src_bytes / elapsed_seconds / 1048576.0);

    /// If there was no data, and we aggregate without keys, we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (total_src_rows == 0 && params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
        aggregator.executeOnBlock(
            children.at(0)->getHeader(),
            *many_data[0],
            threads_data[0].key_columns,
            threads_data[0].aggregate_columns);
}

void ParallelAggregatingBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    buffer.fmtAppend(", max_threads: {}, final: {}", max_threads, final ? "true" : "false");
}

uint64_t ParallelAggregatingBlockInputStream::collectCPUTimeNsImpl(bool is_thread_runner)
{
    uint64_t cpu_time_ns = impl ? impl->collectCPUTimeNs(is_thread_runner) : 0;
    // Each of ParallelAggregatingBlockInputStream's children is a thread-runner.
    forEachChild([&](IBlockInputStream & child) {
        cpu_time_ns += child.collectCPUTimeNs(true);
        return false;
    });
    return cpu_time_ns;
}

} // namespace DB
