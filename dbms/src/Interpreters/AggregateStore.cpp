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

#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <Interpreters/AggregateStore.h>

namespace ProfileEvents
{
extern const Event ExternalAggregationMerge;
}

namespace DB
{
AggregateStore::AggregateStore(
    const String & req_id,
    const Aggregator::Params & params,
    const FileProviderPtr & file_provider_,
    bool is_final_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : file_provider(file_provider_)
    , is_final(is_final_)
    , max_threads(max_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , log(Logger::get("AggregateStore", req_id))
    , aggregator(params, req_id)
{
    assert(max_threads > 0);
    assert(temporary_data_merge_threads > 0);

    many_data.reserve(max_threads);
    for (auto & elem : many_data)
        elem = std::make_shared<AggregatedDataVariants>();

    threads_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
        threads_data.emplace_back(params.keys_size, params.aggregates_size);
}

Block AggregateStore::getHeader() const
{
    return aggregator.getHeader(is_final);
}

const Aggregator::Params & AggregateStore::getParams() const
{
    return aggregator.getParams();
}

const AggregatedDataVariantsPtr & AggregateStore::getData(size_t index) const
{
    assert(index < max_threads);
    return many_data[index];
}

const ThreadData & AggregateStore::getThreadData(size_t index) const
{
    assert(index < max_threads);
    return threads_data[index];
}

void AggregateStore::executeOnBlock(size_t index, const Block & block)
{
    assert(index < max_threads);
    auto & thread_data = threads_data[index];
    aggregator.executeOnBlock(
        block,
        *many_data[index],
        file_provider,
        thread_data.key_columns,
        thread_data.aggregate_columns,
        thread_data.local_delta_memory,
        thread_data.no_more_keys);

    thread_data.src_rows += block.rows();
    thread_data.src_bytes += block.bytes();
}

void AggregateStore::tryFlush(size_t index)
{
    if (aggregator.hasTemporaryFiles())
    {
        /// Flush data in the RAM to disk. So it's easier to unite them later.
        auto & data = *getData(index);

        if (data.isConvertibleToTwoLevel())
            data.convertToTwoLevel();

        if (!data.empty())
            aggregator.writeToTemporaryFile(data, file_provider);
    }
}

void AggregateStore::tryFlush()
{
    if (aggregator.hasTemporaryFiles())
    {
        /// It may happen that some data has not yet been flushed,
        ///  because at the time of `onFinishThread` call, no data has been flushed to disk, and then some were.
        for (const auto & data : many_data)
        {
            if (data->isConvertibleToTwoLevel())
                data->convertToTwoLevel();

            if (!data->empty())
                aggregator.writeToTemporaryFile(*data, file_provider);
        }
    }
}

std::unique_ptr<IBlockInputStream> AggregateStore::merge()
{
    if (!aggregator.hasTemporaryFiles())
    {
        /** If all partially-aggregated data is in RAM, then merge them in parallel, also in RAM.
                */
        return aggregator.mergeAndConvertToBlocks(many_data, is_final, max_threads);
    }
    else
    {
        /** If there are temporary files with partially-aggregated data on the disk,
                *  then read and merge them, spending the minimum amount of memory.
                */

        ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

        const auto & files = aggregator.getTemporaryFiles();
        BlockInputStreams input_streams;
        for (const auto & file : files.files)
        {
            temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path(), file_provider));
            input_streams.emplace_back(temporary_inputs.back()->block_in);
        }

        LOG_FMT_TRACE(
            log,
            "Will merge {} temporary files of size {:.2f} MiB compressed, {:.2f} MiB uncompressed.",
            files.files.size(),
            (files.sum_size_compressed / 1048576.0),
            (files.sum_size_uncompressed / 1048576.0));

        return std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(
            input_streams,
            getParams(),
            is_final,
            temporary_data_merge_threads,
            temporary_data_merge_threads,
            log->identifier());
    }
}

std::pair<size_t, size_t> AggregateStore::mergeSrcRowsAndBytes() const
{
    size_t total_src_rows = 0;
    size_t total_src_bytes = 0;
    for (const auto & thread_data : threads_data)
    {
        total_src_rows += thread_data.src_rows;
        total_src_bytes += thread_data.src_bytes;
    }
    return {total_src_rows, total_src_bytes};
}
} // namespace DB
