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

#include <Common/FailPoint.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/MergingBuckets.h>

namespace DB
{
namespace FailPoints
{
extern const char random_aggregate_merge_failpoint[];
} // namespace FailPoints

#define AggregationMethodName(NAME) AggregatedDataVariants::AggregationMethod_##NAME
#define AggregationMethodType(NAME) AggregatedDataVariants::Type::NAME
#define ToAggregationMethodPtr(NAME, ptr) (reinterpret_cast<AggregationMethodName(NAME) *>(ptr))

MergingBuckets::MergingBuckets(const Aggregator & aggregator_, const ManyAggregatedDataVariants & data_, bool final_, size_t concurrency_)
    : log(Logger::get(aggregator_.log ? aggregator_.log->identifier() : ""))
    , aggregator(aggregator_)
    , data(data_)
    , final(final_)
    , concurrency(concurrency_)
{
    assert(concurrency > 0);
    if (!data.empty())
    {
        is_two_level = data[0]->isTwoLevel();
        if (is_two_level)
        {
            for (size_t i = 0; i < concurrency; ++i)
                two_level_parallel_merge_data.push_back(std::make_unique<BlocksList>());
        }
        // for single level, concurrency must be 1.
        RUNTIME_CHECK(is_two_level || concurrency == 1);

        /// At least we need one arena in first data item per concurrency
        if (concurrency > data[0]->aggregates_pools.size())
        {
            Arenas & first_pool = data[0]->aggregates_pools;
            for (size_t j = first_pool.size(); j < concurrency; ++j)
                first_pool.emplace_back(std::make_shared<Arena>());
        }
    }
}

Block MergingBuckets::getHeader() const
{
    return aggregator.getHeader(final);
}

Block MergingBuckets::getData(size_t concurrency_index)
{
    assert(concurrency_index < concurrency);

    if (unlikely(data.empty()))
        return {};

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_aggregate_merge_failpoint);

    return is_two_level ? getDataForTwoLevel(concurrency_index) : getDataForSingleLevel();
}

Block MergingBuckets::getDataForSingleLevel()
{
    assert(!data.empty());

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
            return popBlocksListFront(single_level_blocks);
        }
    }

    Block out_block = popBlocksListFront(single_level_blocks);
    if (likely(out_block))
    {
        return out_block;
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
    return popBlocksListFront(single_level_blocks);
}

Block MergingBuckets::getDataForTwoLevel(size_t concurrency_index)
{
    assert(concurrency_index < two_level_parallel_merge_data.size());
    auto & two_level_merge_data = *two_level_parallel_merge_data[concurrency_index];

    Block out_block = popBlocksListFront(two_level_merge_data);
    if (likely(out_block))
        return out_block;

    if (current_bucket_num >= NUM_BUCKETS)
        return {};
    while (true)
    {
        auto local_current_bucket_num = current_bucket_num.fetch_add(1);
        // -1 only makes sense for single level, when current_bucket_num is equal to -1, two level merge is skipped directly
        if (unlikely(local_current_bucket_num == -1))
        {
            local_current_bucket_num = current_bucket_num.fetch_add(1);
            assert(local_current_bucket_num > 0);
        }
        if (unlikely(local_current_bucket_num >= NUM_BUCKETS))
            return {};

        doLevelMerge(local_current_bucket_num, concurrency_index);
        Block out_block = popBlocksListFront(two_level_merge_data);
        if (likely(out_block))
            return out_block;
    }
}

void MergingBuckets::doLevelMerge(Int32 bucket_num, size_t concurrency_index)
{
    auto & two_level_merge_data = *two_level_parallel_merge_data[concurrency_index];
    assert(two_level_merge_data.empty());

    assert(!data.empty());
    auto & merged_data = *data[0];
    auto method = merged_data.type;

    /// Select Arena to avoid race conditions
    Arena * arena = merged_data.aggregates_pools.at(concurrency_index).get();

#define M(NAME)                                                                           \
    case AggregationMethodType(NAME):                                                     \
    {                                                                                     \
        aggregator.mergeBucketImpl<AggregationMethodName(NAME)>(data, bucket_num, arena); \
        two_level_merge_data = aggregator.convertOneBucketToBlocks(                       \
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
}

#undef AggregationMethodName
#undef AggregationMethodType
#undef ToAggregationMethodPtr
} // namespace DB
