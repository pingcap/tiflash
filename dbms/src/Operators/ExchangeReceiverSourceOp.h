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
        const std::shared_ptr<ExchangeReceiver> & remote_reader_,
        size_t stream_id_,
        const String & req_id)
        : SourceOp(exec_status_)
        , remote_reader(remote_reader_)
        , stream_id(stream_id_)
        , log(Logger::get(req_id))
    {
        setHeader(Block(getColumnWithTypeAndName(toNamesAndTypes(remote_reader->getOutputSchema()))));
        decoder_ptr = std::make_unique<CHBlockChunkDecodeAndSquash>(getHeader(), 8192);
    }

    String getName() const override
    {
        return "ExchangeReceiverSourceOp";
    }

protected:
    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

    void operateSuffix() noexcept override;

private:
    // TODO support ConnectionProfileInfo.
    std::shared_ptr<ExchangeReceiver> remote_reader;
    std::unique_ptr<CHBlockChunkDecodeAndSquash> decoder_ptr;
    uint64_t total_rows{};
    std::queue<Block> block_queue;
    std::optional<ReceivedMessagePtr> recv_msg;

    size_t stream_id;
    const LoggerPtr log;
};
} // namespace DB
