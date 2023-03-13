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

#include <Interpreters/ExternalAggregator.h>

namespace DB
{
ExternalAggregator::ExternalAggregator(
    const Aggregator::Params & params, 
    const Block & header_, 
    const AggregatedDataVariants::Type & type,
    const String & req_id)
    : is_local_agg(params.is_local_agg)
    , header(header_)
    , bucket_num(AggregatedDataVariants::getBucketNumberForTwoLevelHashTable(type))
    , log(Logger::get(req_id))
    /// for local agg, use sort base spiller.
    /// for non local agg, use partition base spiller.
    , partition_num(is_local_agg ? 1 : bucket_num)
    , spiller(std::make_unique<Spiller>(params.spill_config, /*is_input_sorted=*/is_local_agg, partition_num, header, log))
{}

void ExternalAggregator::setCancellationHook(Aggregator::CancellationHook cancellation_hook)
{
    is_cancelled = cancellation_hook;
}

void ExternalAggregator::sortBaseSpill(std::function<Block(size_t)> && get_bucket_block, std::funtion<void(const Block &)> update_max_sizes)
{
        /// For sort base spilling, we write all the blocks of the bucket to the same partition and guarantee the order of the buckets.
        /// And then restore all blocks from the same partition for multi-bucket and use sort base merging.
        assert(partition_num == 1);

        Blocks blocks;
        for (size_t bucket = 0; bucket < bucket_num; ++bucket)
        {
            /// memory in hash table is released after `convertOneBucketToBlock`,
            /// so the peak memory usage is not increased although we save all
            /// the blocks before the actual spill
            blocks.push_back(get_bucket_block(bucket));
            update_max_sizes(blocks.back());
        }
        spiller->spillBlocks(std::move(blocks), 0);
}

void ExternalAggregator::partitionBaseSpill(std::function<Block(size_t)> && get_bucket_block, std::funtion<void(const Block &)> update_max_sizes)
{
        /// For partition base spilling, blocks of different buckets are written to different partitions.
        /// And then the blocks in multi-buckets are recovered in parallel.
        assert(partition_num == bucket_num);

        // To avoid multiple threads spilling the same partition at the same time.
        // This will allow the spiller's `append` to take effect as far as possible.
        std::uniform_int_distribution<> dist(0, bucket_num - 1);
        size_t bucket_index = dist(gen);
        auto next_bucket_index = [&]() {
            auto tmp = bucket_index++;
            if (bucket_index == bucket_num)
                bucket_index = 0;
            return tmp;
        };
        for (size_t i = 0; i < bucket_num; ++i)
        {
            auto bucket = next_bucket_index();
            /// memory in hash table is released after `convertOneBucketToBlock`, so the peak memory usage is not increased here.
            auto bucket_block = get_bucket_block(bucket);
            update_max_sizes(bucket_block);
            spiller->spillBlocks({std::move(bucket_block)}, bucket);
        }
}

void ExternalAggregator::spill(std::function<Block(size_t)> && get_bucket_block)
{
    size_t max_temporary_block_size_rows = 0;
    size_t max_temporary_block_size_bytes = 0;s
    auto update_max_sizes = [&](const Block & block) {
        size_t block_size_rows = block.rows();
        size_t block_size_bytes = block.bytes();

        if (block_size_rows > max_temporary_block_size_rows)
            max_temporary_block_size_rows = block_size_rows;
        if (block_size_bytes > max_temporary_block_size_bytes)
            max_temporary_block_size_bytes = block_size_bytes;
    };

    if (is_local_agg)
        sortBaseSpill(get_bucket_block, std::move(update_max_sizes));
    else
        partitionBaseSpill(get_bucket_block, std::move(update_max_sizes));

    LOG_TRACE(log, "Max size of temporary bucket blocks: {} rows, {:.3f} MiB.", max_temporary_block_size_rows, (max_temporary_block_size_bytes / 1048576.0));
}

void ExternalAggregator::finishSpill()
{
    spiller->finishSpill();
    prepareForRestore();
}

void ExternalAggregator::prepareForRestore()
{
    assert(restored_inputs.empty());
    if (is_local_agg)
    {
        /// Sort base preapre
        /// An ordered input stream is an input.
        RUNTIME_CHECK(partition_num == 1);
        for (const auto & input : spiller->restoreBlocks(0))
            restored_inputs.emplace_back(input);
    }
    else
    {
        /// Partition base preapre
        /// The inputstream of a bucket is an input.
        for (size_t partition_id = 0; partition_id < partition_num; ++partition_id)
        {
            auto ret = spiller->restoreBlocks(partition_id, /*max_stream_size=*/1);
            RUNTIME_CHECK(ret.size() == 1);
            restored_inputs.emplace_back(ret.back());
        }
    }
}

bool ExternalAggregator::hasSpilledData() const
{
    return spiller->hasSpilledData();
}

bool ExternalAggregator::hasRestoreData() const
{
    return !restored_inputs.empty();
}

BlocksList ExternalAggregator::restoreBucketBlocks()
{
    if unlikely (is_cancelled())
        return {};

    if unlikely (restored_inputs.empty())
        return {};

    if (current_bucket_num >= bucket_num)
        return {};

    return is_local_agg ? sortBaseRestore() : partitionBaseRestore();
}

BlocksList ExternalAggregator::sortBaseRestore()
{
    while (true)
    {
        auto local_bucket_num = current_bucket_num.fetch_add(1);
        if (local_bucket_num >= bucket_num)
            return {};

        BlocksList ret;
        for (auto & input : restored_inputs)
        {
            if unlikely (is_cancelled())
                return {};
            input.next();
            if (input.hasOutput() && input.getOutputBucketNum() == local_bucket_num)
                ret.push_back(input.moveOutput());
        }
        if unlikely (is_cancelled())
            return {};
        if (ret.empty())
            continue;
        return ret;
    }
}

BlocksList ExternalAggregator::partitionBaseRestore()
{
    while (true)
    {
        auto local_bucket_num = current_bucket_num.fetch_add(1);
        if (local_bucket_num >= bucket_num)
            return {};
    
        BlocksList ret;
        auto & cur_input = restored_inputs[local_bucket_num];
        while (!cur_input.empty())
        {
            if unlikely (is_cancelled())
                return {};
            cur_input.next();
            if (cur_input.hasOutput())
                ret.push_back(cur_input.moveOutput());
        }
        if unlikely (is_cancelled())
            return {};
        if (ret.empty())
            continue;
        return ret;
    }
}

ExternalAggregator::Input::Input(const BlockInputStreamPtr & stream_): stream(stream_) { stream->readPrefix(); }

bool ExternalAggregator::Input::hasOutput() const { return output.has_value(); }
Block ExternalAggregator::Input::moveOutput()
{
    assert(hasOutput());
    Block ret = std::move(*output);
    output.reset();
    return ret;
}
size_t ExternalAggregator::Input::getOutputBucketNum() const
{
    assert(hasOutput());
    return static_cast<size_t>(output->info.bucket_num);
}

void ExternalAggregator::Input::next()
{
    if (empty())
        return;

    if (hasOutput())
        return;

    Block ret = stream->read();
    if (!ret)
    {
        is_exhausted = true;
    }
    else
    {
        /// Only two level data can be spilled.
        assert(ret.info.bucket_num != -1);
        output.emplace(std::move(ret));
    }
}

bool ExternalAggregator::Input::empty() const { return is_exhausted; }
} // namespace DB
