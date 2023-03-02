// Copyright 2023 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Operators/Operator.h>

namespace DB
{
class ExchangeReceiverSourceOp : public SourceOp
{
public:
    ExchangeReceiverSourceOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id,
        const std::shared_ptr<ExchangeReceiver> & exchange_receiver_,
        size_t stream_id_)
        : SourceOp(exec_status_, req_id)
        , exchange_receiver(exchange_receiver_)
        , stream_id(stream_id_)
    {
        setHeader(Block(getColumnWithTypeAndName(toNamesAndTypes(exchange_receiver->getOutputSchema()))));
        decoder_ptr = std::make_unique<CHBlockChunkDecodeAndSquash>(getHeader(), 8192);
    }

    String getName() const override
    {
        return "ExchangeReceiverSourceOp";
    }

    void operateSuffix() override;

protected:
    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

private:
    Block popFromBlockQueue();

private:
    // TODO support ConnectionProfileInfo.
    // TODO support RemoteExecutionSummary.
    std::shared_ptr<ExchangeReceiver> exchange_receiver;
    std::unique_ptr<CHBlockChunkDecodeAndSquash> decoder_ptr;
    uint64_t total_rows{};
    std::queue<Block> block_queue;
    std::optional<ReceiveResult> recv_res;

    size_t stream_id;
};
} // namespace DB
