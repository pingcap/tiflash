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
#include <Flash/Coprocessor/RemoteExecutionSummary.h>
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
        DAGContext & dag_context_,
        const String & executor_id_,
        size_t stream_id_);

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
    std::shared_ptr<ExchangeReceiver> exchange_receiver;
    std::unique_ptr<CHBlockChunkDecodeAndSquash> decoder_ptr;
    uint64_t total_rows{};
    std::queue<Block> block_queue;
    std::optional<ReceiveResult> recv_res;

    DAGContext & dag_context;
    ConnectionProfiles connection_profiles;
    RemoteExecutionSummary remote_execution_summary;

    String executor_id;
    size_t stream_id;
};
} // namespace DB
