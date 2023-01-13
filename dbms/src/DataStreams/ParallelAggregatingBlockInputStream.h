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
#include <DataStreams/ParallelInputsProcessor.h>
#include <DataStreams/TemporaryFileStream.h>

namespace DB
{
/** Aggregates several sources in parallel.
  * Makes aggregation of blocks from different sources independently in different threads, then combines the results.
  * If final == false, aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  */
class ParallelAggregatingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "ParallelAggregating";

public:
    /** Columns from key_names and arguments of aggregate functions must already be computed.
      */
    ParallelAggregatingBlockInputStream(
        const BlockInputStreams & inputs,
        const BlockInputStreams & additional_inputs_at_end,
        const Aggregator::Params & params_,
        const FileProviderPtr & file_provider_,
        bool final_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_,
        const String & req_id);

    String getName() const override { return NAME; }

    void cancel(bool kill) override;

    Block getHeader() const override;

    void collectNewThreadCountOfThisLevel(int & cnt) override
    {
        cnt += processor.getMaxThreads();
    }

protected:
    /// Do nothing that preparation to execution of the query be done in parallel, in ParallelInputsProcessor.
    void readPrefix() override
    {
    }

    Block readImpl() override;
    void appendInfo(FmtBuffer & buffer) const override;


private:
    const LoggerPtr log;

    Aggregator::Params params;
    Aggregator aggregator;
    FileProviderPtr file_provider;
    bool final;
    size_t max_threads;
    size_t temporary_data_merge_threads;

    size_t keys_size;
    size_t aggregates_size;

    std::atomic<bool> executed{false};

    TemporaryFileStreams temporary_inputs;

    ManyAggregatedDataVariants many_data;
    Exceptions exceptions;
    std::atomic<Int32> first_exception_index{-1};

    struct ThreadData
    {
        size_t src_rows = 0;
        size_t src_bytes = 0;
        Int64 local_delta_memory = 0;

        ColumnRawPtrs key_columns;
        Aggregator::AggregateColumns aggregate_columns;

        ThreadData(size_t keys_size, size_t aggregates_size)
        {
            key_columns.resize(keys_size);
            aggregate_columns.resize(aggregates_size);
        }
    };

    std::vector<ThreadData> threads_data;


    struct Handler
    {
        explicit Handler(ParallelAggregatingBlockInputStream & parent_)
            : parent(parent_)
        {}

        void onBlock(Block & block, size_t thread_num);
        void onFinishThread(size_t thread_num);
        void onFinish();
        void onException(std::exception_ptr & exception, size_t thread_num);
        static String getName()
        {
            return "ParallelAgg";
        }

        ParallelAggregatingBlockInputStream & parent;
    };

    Handler handler;
    ParallelInputsProcessor<Handler> processor;


    void execute();


    /** From here we get the finished blocks after the aggregation.
      */
    std::unique_ptr<IBlockInputStream> impl;
};

} // namespace DB
