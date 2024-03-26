// Copyright 2023 PingCAP, Inc.
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
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const std::shared_ptr<ExchangeReceiver> & exchange_receiver_,
        size_t stream_id_)
        : SourceOp(exec_context_, req_id)
        , exchange_receiver(exchange_receiver_)
        , stream_id(stream_id_)
        , io_profile_info(IOProfileInfo::createForRemote(profile_info_ptr, exchange_receiver->getSourceNum()))
    {
        exchange_receiver->verifyStreamId(stream_id);
        setHeader(Block(getColumnWithTypeAndName(toNamesAndTypes(exchange_receiver->getOutputSchema()))));
        decoder_ptr = std::make_unique<CHBlockChunkDecodeAndSquash>(getHeader(), 8192);
    }

    String getName() const override { return "ExchangeReceiverSourceOp"; }

    IOProfileInfoPtr getIOProfileInfo() const override { return io_profile_info; }

protected:
    void operateSuffixImpl() override;

    ReturnOpStatus readImpl(Block & block) override;

    ReturnOpStatus awaitImpl() override;

private:
    Block popFromBlockQueue();

private:
    std::shared_ptr<ExchangeReceiver> exchange_receiver;
    std::unique_ptr<CHBlockChunkDecodeAndSquash> decoder_ptr;
    uint64_t total_rows{};
    std::queue<Block> block_queue;

    ReceivedMessagePtr recv_msg = nullptr;
    ReceiveStatus receive_status = ReceiveStatus::empty;

    size_t stream_id;

    IOProfileInfoPtr io_profile_info;
};
} // namespace DB
