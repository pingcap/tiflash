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

#include <Flash/Mpp/AsyncWriter.h>
#include <Flash/Mpp/newMPPExchangeWriter.h>
#include <Transforms/Sink.h>

namespace DB
{
class ExchangeSenderSink : public Sink
{
public:
    explicit ExchangeSenderSink(
        const MPPTunnelSetPtr writer_,
        const std::vector<Int64> & partition_col_ids_,
        const TiDB::TiDBCollators & collators_,
        const tipb::ExchangeType & exchange_type_,
        Int64 records_per_chunk_,
        Int64 batch_send_min_limit_,
        DAGContext & dag_context_)
        : async_writer(newMPPExchangeAsyncWriter(
            writer_,
            partition_col_ids_,
            collators_,
            exchange_type_,
            records_per_chunk_,
            batch_send_min_limit_,
            dag_context_))
    {}

    bool write(Block & block) override
    {
        if (unlikely(!block))
            return false;

        async_writer->asyncWrite(std::move(block));
        return true;
    }

    bool finish() override
    {
        return async_writer->asyncFinishWrite();
    }

    bool isReady() override
    {
        return async_writer->asyncIsReady();
    }

private:
    AsyncWriterPtr async_writer;
};
} // namespace DB
