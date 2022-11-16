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

#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/FineGrainedShuffleWriter.h>
#include <Flash/Mpp/HashPartitionWriter.h>

namespace DB
{
template <class StreamWriterPtr>
std::unique_ptr<DAGResponseWriter> newMPPExchangeWriter(
    const StreamWriterPtr & writer,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & partition_col_collators,
    const tipb::ExchangeType & exchange_type,
    Int64 records_per_chunk,
    Int64 batch_send_min_limit,
    bool should_send_exec_summary_at_last,
    DAGContext & dag_context,
    bool enable_fine_grained_shuffle,
    UInt64 fine_grained_shuffle_stream_count,
    UInt64 fine_grained_shuffle_batch_size)
{
    RUNTIME_CHECK(dag_context.isMPPTask());
    should_send_exec_summary_at_last = dag_context.collect_execution_summaries && should_send_exec_summary_at_last;
    if (dag_context.isRootMPPTask())
    {
        RUNTIME_CHECK(!enable_fine_grained_shuffle);
        RUNTIME_CHECK(exchange_type == tipb::ExchangeType::PassThrough);
        return std::make_unique<StreamingDAGResponseWriter<StreamWriterPtr>>(
            writer,
            records_per_chunk,
            batch_send_min_limit,
            should_send_exec_summary_at_last,
            dag_context);
    }
    else
    {
        if (exchange_type == tipb::ExchangeType::Hash)
        {
            if (enable_fine_grained_shuffle)
            {
                return std::make_unique<FineGrainedShuffleWriter<StreamWriterPtr>>(
                    writer,
                    partition_col_ids,
                    partition_col_collators,
                    should_send_exec_summary_at_last,
                    dag_context,
                    fine_grained_shuffle_stream_count,
                    fine_grained_shuffle_batch_size);
            }
            else
            {
                return std::make_unique<HashPartitionWriter<StreamWriterPtr>>(
                    writer,
                    partition_col_ids,
                    partition_col_collators,
                    batch_send_min_limit,
                    should_send_exec_summary_at_last,
                    dag_context);
            }
        }
        else
        {
            RUNTIME_CHECK(!enable_fine_grained_shuffle);
            return std::make_unique<BroadcastOrPassThroughWriter<StreamWriterPtr>>(
                writer,
                batch_send_min_limit,
                should_send_exec_summary_at_last,
                dag_context);
        }
    }
}
} // namespace DB
