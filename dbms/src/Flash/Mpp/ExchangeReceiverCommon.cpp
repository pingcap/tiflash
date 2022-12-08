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

#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Mpp/ExchangeReceiverCommon.h>

#include <magic_enum.hpp>
#include <mutex>

#include "common/defines.h"

namespace DB
{
namespace
{
String constructStatusString(ExchangeReceiverState state, const String & error_message)
{
    if (error_message.empty())
        return fmt::format("Receiver state: {}", magic_enum::enum_name(state));
    return fmt::format("Receiver state: {}, error message: {}", magic_enum::enum_name(state), error_message);
}
} // namespace

ExchangeReceiverBase::~ExchangeReceiverBase()
{
    try
    {
        auto myid = std::this_thread::get_id();
        std::stringstream ss;
        ss << myid;
        std::string tid = ss.str();

        auto * logg = &Poco::Logger::get("LRUCache");
        LOG_INFO(logg, "ExchangeRecvBase begins to destruct 1, {}", tid);
        close();
        LOG_INFO(logg, "ExchangeRecvBase begins to destruct 2, {}", tid);
        waitAllLocalConnDone();
        LOG_INFO(logg, "ExchangeRecvBase begins to destruct 3, {}", tid);
        thread_manager->wait();
        LOG_INFO(logg, "ExchangeRecvBase is completely destructed, {}", tid);
    }
    catch (...)
    {
        tryLogCurrentException(exc_log, __PRETTY_FUNCTION__);
    }
}

void ExchangeReceiverBase::waitAllLocalConnDone()
{
    std::unique_lock lock(mu);
    auto pred = [&] {
        return local_conn_num == 0;
    };
    local_conn_cv.wait(lock, pred);
}

void ExchangeReceiverBase::prepareMsgChannels()
{
    if (enable_fine_grained_shuffle_flag)
        for (size_t i = 0; i < output_stream_count; ++i)
            msg_channels.push_back(std::make_shared<MPMCQueue<std::shared_ptr<ReceivedMessage>>>(max_buffer_size));
    else
        msg_channels.push_back(std::make_shared<MPMCQueue<std::shared_ptr<ReceivedMessage>>>(max_buffer_size));
}

void ExchangeReceiverBase::cancel()
{
    setEndState(ExchangeReceiverState::CANCELED);
    cancelAllMsgChannels();
}

void ExchangeReceiverBase::close()
{
    setEndState(ExchangeReceiverState::CLOSED);
    finishAllMsgChannels();
}

DecodeDetail ExchangeReceiverBase::decodeChunks(
    const std::shared_ptr<ReceivedMessage> & recv_msg,
    std::queue<Block> & block_queue,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    DecodeDetail detail;

    if (recv_msg->chunks.empty())
        return detail;
    auto & packet = recv_msg->packet->packet;

    // Record total packet size even if fine grained shuffle is enabled.
    detail.packet_bytes = packet.ByteSizeLong();
    for (const String * chunk : recv_msg->chunks)
    {
        auto result = decoder_ptr->decodeAndSquash(*chunk);
        if (!result)
            continue;
        detail.rows += result->rows();
        if likely (result->rows() > 0)
        {
            block_queue.push(std::move(result.value()));
        }
    }
    return detail;
}

ExchangeReceiverResult ExchangeReceiverBase::nextResult(
    std::queue<Block> & block_queue,
    const Block & header,
    size_t stream_id,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    if (unlikely(stream_id >= msg_channels.size()))
    {
        LOG_ERROR(exc_log, "stream_id out of range, stream_id: {}, total_stream_count: {}", stream_id, msg_channels.size());
        return ExchangeReceiverResult::newError(0, "", "stream_id out of range");
    }

    auto myid = std::this_thread::get_id();
    std::stringstream ss;
    ss << myid;
    std::string tid = ss.str();

    std::shared_ptr<ReceivedMessage> recv_msg;
    if (msg_channels[stream_id]->pop(recv_msg) != MPMCQueueResult::OK)
    {
        return handleUnnormalChannel(block_queue, decoder_ptr);
    }
    else
    {
        assert(recv_msg != nullptr);
        if (unlikely(recv_msg->error_ptr != nullptr))
            return ExchangeReceiverResult::newError(recv_msg->source_index, recv_msg->req_info, recv_msg->error_ptr->msg());

        return toDecodeResult(block_queue, header, recv_msg, decoder_ptr);
    }
}

ExchangeReceiverResult ExchangeReceiverBase::handleUnnormalChannel(
    std::queue<Block> & block_queue,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    std::optional<Block> last_block = decoder_ptr->flush();
    std::unique_lock lock(mu);
    if (this->state != DB::ExchangeReceiverState::NORMAL)
    {
        return DB::ExchangeReceiverResult::newError(0, DB::ExchangeReceiverBase::name, DB::constructStatusString(this->state, this->err_msg));
    }
    else
    {
        /// If there are cached data in squashDecoder, then just push the block and return EOF next iteration
        if (last_block && last_block->rows() > 0)
        {
            /// Can't get correct caller_index here, use 0 instead
            auto result = ExchangeReceiverResult::newOk(nullptr, 0, "");
            result.decode_detail.packets = 0;
            result.decode_detail.rows = last_block->rows();
            block_queue.push(std::move(last_block.value()));
            return result;
        }
        else
        {
            return DB::ExchangeReceiverResult::newEOF(DB::ExchangeReceiverBase::name); /// live_connections == 0, msg_channel is finished, and state is NORMAL, that is the end.
        }
    }
}

ExchangeReceiverResult ExchangeReceiverBase::toDecodeResult(
    std::queue<Block> & block_queue,
    const Block & header,
    const std::shared_ptr<ReceivedMessage> & recv_msg,
    std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr)
{
    assert(recv_msg != nullptr);
    if (recv_msg->resp_ptr != nullptr) /// the data of the last packet is serialized from tipb::SelectResponse including execution summaries.
    {
        auto select_resp = std::make_shared<tipb::SelectResponse>();
        if (unlikely(!select_resp->ParseFromString(*(recv_msg->resp_ptr))))
        {
            return ExchangeReceiverResult::newError(recv_msg->source_index, recv_msg->req_info, "decode error");
        }
        else
        {
            auto result = ExchangeReceiverResult::newOk(select_resp, recv_msg->source_index, recv_msg->req_info);
            /// If mocking TiFlash as TiDB, we should decode chunks from select_resp.
            if (unlikely(!result.resp->chunks().empty()))
            {
                assert(recv_msg->chunks.empty());
                // Fine grained shuffle should only be enabled when sending data to TiFlash node.
                // So all data should be encoded into MPPDataPacket.chunks.
                RUNTIME_CHECK_MSG(!enable_fine_grained_shuffle_flag, "Data should not be encoded into tipb::SelectResponse.chunks when fine grained shuffle is enabled");
                result.decode_detail = CoprocessorReader::decodeChunks(select_resp, block_queue, header, schema);
            }
            else if (!recv_msg->chunks.empty())
            {
                result.decode_detail = decodeChunks(recv_msg, block_queue, decoder_ptr);
            }
            return result;
        }
    }
    else /// the non-last packets
    {
        auto result = ExchangeReceiverResult::newOk(nullptr, recv_msg->source_index, recv_msg->req_info);
        result.decode_detail = decodeChunks(recv_msg, block_queue, decoder_ptr);
        return result;
    }
}

bool ExchangeReceiverBase::setEndState(ExchangeReceiverState new_state)
{
    assert(new_state == ExchangeReceiverState::CANCELED || new_state == ExchangeReceiverState::CLOSED);
    std::unique_lock lock(mu);
    if (state == ExchangeReceiverState::CANCELED || state == ExchangeReceiverState::CLOSED)
    {
        return false;
    }
    state = new_state;
    return true;
}

String ExchangeReceiverBase::getStatusString()
{
    std::unique_lock lock(mu);
    return constructStatusString(state, err_msg);
}

void ExchangeReceiverBase::connectionDone(
    bool meet_error,
    const String & local_err_msg,
    const LoggerPtr & log)
{
    auto myid = std::this_thread::get_id();
    std::stringstream ss;
    ss << myid;
    std::string tid = ss.str();

    auto * logg = &Poco::Logger::get("LRUCache");
    Int32 copy_live_conn = -1;
    {
        std::unique_lock lock(mu);
        if (meet_error)
        {
            if (state == ExchangeReceiverState::NORMAL)
                state = ExchangeReceiverState::ERROR;
            if (err_msg.empty())
                err_msg = local_err_msg;
        }
        copy_live_conn = --live_connections;
        LOG_INFO(logg, "TLocal: connectionLocalDone, local_conn_num {}, meet_error {}, copy_live_conn {}, {}", local_conn_num, meet_error, copy_live_conn, tid);
    }
    LOG_DEBUG(
        log,
        "Remote connection end. meet error: {}, err msg: {}, current alive connections: {}",
        meet_error,
        local_err_msg,
        copy_live_conn);

    if (copy_live_conn == 0)
        LOG_DEBUG(log, "All threads end in ExchangeReceiver");
    else if (unlikely(copy_live_conn < 0))
        throw Exception("live_connections should not be less than 0!");

    if (meet_error || copy_live_conn == 0)
        finishAllMsgChannels();
}

void ExchangeReceiverBase::connectionLocalDone(
    bool meet_error,
    const String & local_err_msg,
    const LoggerPtr & log)
{
    auto myid = std::this_thread::get_id();
    std::stringstream ss;
    ss << myid;
    std::string tid = ss.str();

    auto * logg = &Poco::Logger::get("LRUCache");
    LOG_INFO(logg, "TLocal: connectionLocalDone, {}", tid);
    Int32 copy_live_conn = -1;
    {
        std::unique_lock lock(mu);
        if (meet_error)
        {
            if (state == ExchangeReceiverState::NORMAL)
                state = ExchangeReceiverState::ERROR;
            if (err_msg.empty())
                err_msg = local_err_msg;
        }
        copy_live_conn = --live_connections;
        --local_conn_num;
        LOG_INFO(logg, "TLocal: connectionLocalDone, local_conn_num {}, meet_error {}, copy_live_conn {}, {}", local_conn_num, meet_error, copy_live_conn, tid);
        local_conn_cv.notify_all();
    }

    LOG_DEBUG(
        log,
        "Local connection end. meet error: {}, err msg: {}, all current alive connections {}, alive local connections {}",
        meet_error,
        local_err_msg,
        copy_live_conn,
        local_conn_num);

    if (copy_live_conn == 0)
        LOG_DEBUG(log, "All threads end in ExchangeReceiver");
    else if (unlikely(copy_live_conn < 0))
        throw Exception("live_connections should not be less than 0!");

    if (meet_error || copy_live_conn == 0)
        finishAllMsgChannels();
}

void ExchangeReceiverBase::finishAllMsgChannels()
{
    for (auto & msg_channel : msg_channels)
        msg_channel->finish();
}

void ExchangeReceiverBase::cancelAllMsgChannels()
{
    for (auto & msg_channel : msg_channels)
        msg_channel->cancel();
}

} // namespace DB