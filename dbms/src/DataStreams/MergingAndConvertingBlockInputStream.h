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

#include <Common/FailPoint.h>
#include <Common/ThreadManager.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Aggregator.h>

namespace DB
{
namespace FailPoints
{
extern const char random_aggregate_merge_failpoint[];
} // namespace FailPoints

#define AggregationMethodName(NAME) AggregatedDataVariants::AggregationMethod_##NAME
#define AggregationMethodType(NAME) AggregatedDataVariants::Type::NAME
#define ToAggregationMethodPtr(NAME, ptr) (reinterpret_cast<AggregationMethodName(NAME) *>(ptr))

/** Combines aggregation states together, turns them into blocks, and outputs streams.
  * If the aggregation states are two-level, then it produces blocks strictly in order of 'bucket_num'.
  * (This is important for distributed processing.)
  * In doing so, it can handle different buckets in parallel, using up to `threads` threads.
  */
class MergingAndConvertingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** The input is a set of non-empty sets of partially aggregated data,
      *  which are all either single-level, or are two-level.
      */
    MergingAndConvertingBlockInputStream(const Aggregator & aggregator_, ManyAggregatedDataVariants & data_, bool final_, size_t threads_)
        : log(Logger::get(aggregator_.log ? aggregator_.log->identifier() : ""))
        , aggregator(aggregator_)
        , data(data_)
        , final(final_)
        , threads(threads_)
    {
        /// At least we need one arena in first data item per thread
        if (!data.empty() && threads > data[0]->aggregates_pools.size())
        {
            Arenas & first_pool = data[0]->aggregates_pools;
            for (size_t j = first_pool.size(); j < threads; ++j)
                first_pool.emplace_back(std::make_shared<Arena>());
        }
    }

    String getName() const override { return "MergingAndConverting"; }

    Block getHeader() const override { return aggregator.getHeader(final); }

    ~MergingAndConvertingBlockInputStream() override
    {
        LOG_TRACE(&Poco::Logger::get(__PRETTY_FUNCTION__), "Waiting for threads to finish");

        /// We need to wait for threads to finish before destructor of 'parallel_merge_data',
        ///  because the threads access 'parallel_merge_data'.
        if (parallel_merge_data && parallel_merge_data->thread_pool)
            parallel_merge_data->thread_pool->wait();
    }

protected:
    Block readImpl() override
    {
        if (data.empty())
            return {};

        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_aggregate_merge_failpoint);

        AggregatedDataVariantsPtr & first = data[0];

        if (current_bucket_num == -1)
        {
            ++current_bucket_num;

            if (first->type == AggregatedDataVariants::Type::without_key)
            {
                aggregator.mergeWithoutKeyDataImpl(data);
                single_level_blocks = aggregator.prepareBlocksAndFillWithoutKey(
                    *first,
                    final);
                return tryGetSingleLevelOutputBlock();
            }
        }

        if (!first->isTwoLevel())
        {
            if (Block out = tryGetSingleLevelOutputBlock())
            {
                return out;
            }

            if (current_bucket_num > 0)
                return {};

            if (first->type == AggregatedDataVariants::Type::without_key)
                return {};

            ++current_bucket_num;

#define M(NAME)                                                                 \
    case AggregationMethodType(NAME):                                           \
    {                                                                           \
        aggregator.mergeSingleLevelDataImpl<AggregationMethodName(NAME)>(data); \
        break;                                                                  \
    }
            switch (first->type)
            {
                APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
            }
#undef M
            single_level_blocks = aggregator.prepareBlocksAndFillSingleLevel(*first, final);
            return tryGetSingleLevelOutputBlock();
        }
        else
        {
            if (Block out = tryGetTwoLevelOutputBlock())
            {
                return out;
            }

            if (current_bucket_num >= NUM_BUCKETS)
            {
                return {};
            }

            if (!parallel_merge_data)
            {
                parallel_merge_data = std::make_unique<ParallelMergeData>(threads);
                for (size_t i = 0; i < threads; ++i)
                    scheduleThreadForNextBucket();
            }

            Block res;

            while (true)
            {
                std::unique_lock lock(parallel_merge_data->mutex);

                if (parallel_merge_data->exception)
                {
                    std::rethrow_exception(parallel_merge_data->exception);
                }


                if (!parallel_merge_data->ready_blocks.empty())
                {
                    for (auto & ready_block : parallel_merge_data->ready_blocks)
                    {
                        scheduleThreadForNextBucket();
                        two_level_blocks[ready_block.first] = std::move(ready_block.second);
                        current_bucket_num++;
                    }
                    parallel_merge_data->ready_blocks.clear();

                    if (Block out = tryGetTwoLevelOutputBlock())
                    {
                        return out;
                    }
                    else if (current_bucket_num >= NUM_BUCKETS)
                    {
                        return {};
                    }
                }
                parallel_merge_data->condvar.wait(lock);
            };
        }
    }

private:
    const LoggerPtr log;
    const Aggregator & aggregator;
    BlocksList single_level_blocks;
    BucketBlocksListMap two_level_blocks;
    Int32 two_level_blocks_out_bucket = 0;
    ManyAggregatedDataVariants data;
    bool final;
    size_t threads;

    std::atomic<Int32> current_bucket_num = -1;
    std::atomic<Int32> max_scheduled_bucket_num = -1;
    static constexpr Int32 NUM_BUCKETS = 256;

    struct ParallelMergeData
    {
        BucketBlocksListMap ready_blocks;
        std::exception_ptr exception;
        std::mutex mutex;
        std::condition_variable condvar;
        std::shared_ptr<ThreadPoolManager> thread_pool;

        explicit ParallelMergeData(size_t threads)
            : thread_pool(newThreadPoolManager(threads))
        {}
    };

    std::unique_ptr<ParallelMergeData> parallel_merge_data;

    void scheduleThreadForNextBucket()
    {
        int num = max_scheduled_bucket_num.fetch_add(1) + 1;
        if (num >= NUM_BUCKETS)
            return;

        parallel_merge_data->thread_pool->schedule(true, [this, num] { thread(num); });
    }

    void thread(Int32 bucket_num)
    {
        try
        {
            auto & merged_data = *data[0];
            auto method = merged_data.type;
            BlocksList blocks;

            /// Select Arena to avoid race conditions
            size_t thread_number = static_cast<size_t>(bucket_num) % threads;
            Arena * arena = merged_data.aggregates_pools.at(thread_number).get();

#define M(NAME)                                                                           \
    case AggregationMethodType(NAME):                                                     \
    {                                                                                     \
        aggregator.mergeBucketImpl<AggregationMethodName(NAME)>(data, bucket_num, arena); \
        blocks = aggregator.convertOneBucketToBlocks(                                     \
            merged_data,                                                                  \
            *ToAggregationMethodPtr(NAME, merged_data.aggregation_method_impl),           \
            arena,                                                                        \
            final,                                                                        \
            bucket_num);                                                                  \
        break;                                                                            \
    }
            switch (method)
            {
                APPLY_FOR_VARIANTS_TWO_LEVEL(M)
            default:
                break;
            }
#undef M

            std::lock_guard lock(parallel_merge_data->mutex);
            parallel_merge_data->ready_blocks[bucket_num] = std::move(blocks);
        }
        catch (...)
        {
            std::lock_guard lock(parallel_merge_data->mutex);
            if (!parallel_merge_data->exception)
                parallel_merge_data->exception = std::current_exception();
        }

        parallel_merge_data->condvar.notify_all();
    }

    Block tryGetSingleLevelOutputBlock()
    {
        if (!single_level_blocks.empty())
        {
            Block out_block = single_level_blocks.front();
            single_level_blocks.pop_front();
            return out_block;
        }
        return {};
    }

    Block tryGetTwoLevelOutputBlock()
    {
        while (true)
        {
            auto it = two_level_blocks.find(two_level_blocks_out_bucket);
            if (it != two_level_blocks.end())
            {
                if (!it->second.empty())
                {
                    Block out_block = it->second.front();
                    it->second.pop_front();
                    return out_block;
                }
                two_level_blocks_out_bucket++;
            }
            else
            {
                return {};
            }
        }
    }
};

#undef AggregationMethodName
#undef AggregationMethodType
#undef ToAggregationMethodPtr
} // namespace DB
