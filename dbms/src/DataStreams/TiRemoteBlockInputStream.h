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

#include <Common/FmtUtils.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/RemoteExecutionSummary.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <common/logger_useful.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

namespace DB
{
// TiRemoteBlockInputStream is a block input stream that read/receive data from remote.
template <typename RemoteReader>
class TiRemoteBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr bool is_streaming_reader = RemoteReader::is_streaming_reader;

    std::shared_ptr<RemoteReader> remote_reader;
    size_t source_num;
    std::vector<ConnectionProfileInfo> connection_profile_infos;

    Block sample_block;

    std::queue<Block> block_queue;

    String name;

    const LoggerPtr log;

    RemoteExecutionSummary remote_execution_summary;

    uint64_t total_rows;

    // For fine grained shuffle, sender will partition data into muiltiple streams by hashing.
    // ExchangeReceiverBlockInputStream only need to read its own stream, i.e., streams[stream_id].
    // CoprocessorBlockInputStream doesn't take care of this.
    size_t stream_id;

    std::unique_ptr<CHBlockChunkDecodeAndSquash> decoder_ptr;

    bool fetchRemoteResult()
    {
        while (true)
        {
            if unlikely (isCancelled())
                return false;
            auto result = remote_reader->nextResult(block_queue, sample_block, stream_id, decoder_ptr);
            if (result.meet_error)
            {
                LOG_WARNING(log, "remote reader meets error: {}", result.error_msg);
                throw Exception(result.error_msg);
            }
            if (result.eof)
            {
                LOG_DEBUG(log, "remote reader meets eof");
                return false;
            }
            if (result.resp != nullptr && result.resp->has_error())
            {
                LOG_WARNING(log, "remote reader meets error: {}", result.resp->error().DebugString());
                throw Exception(result.resp->error().DebugString());
            }

            /// only the last response contains execution summaries
            if (result.resp != nullptr)
                remote_execution_summary.add(*result.resp);

            size_t index = 0;
            if constexpr (is_streaming_reader)
                index = result.call_index;
            const auto & decode_detail = result.decode_detail;
            auto & connection_profile_info = connection_profile_infos[index];
            connection_profile_info.packets += decode_detail.packets;
            connection_profile_info.bytes += decode_detail.packet_bytes;

            total_rows += decode_detail.rows;
            LOG_TRACE(
                log,
                "recv {} rows from remote for {}, total recv row num: {}",
                decode_detail.rows,
                result.req_info,
                total_rows);

            if (decode_detail.rows > 0)
                return true;
            // else continue
        }
    }

public:
    TiRemoteBlockInputStream(
        std::shared_ptr<RemoteReader> remote_reader_,
        const String & req_id,
        const String & executor_id,
        size_t stream_id_)
        : remote_reader(remote_reader_)
        , source_num(remote_reader->getSourceNum())
        , name(fmt::format("TiRemote({})", RemoteReader::name))
        , log(Logger::get(name, req_id, executor_id))
        , total_rows(0)
        , stream_id(stream_id_)
    {
        connection_profile_infos.resize(source_num);
        sample_block = Block(getColumnWithTypeAndName(toNamesAndTypes(remote_reader->getOutputSchema())));
        static constexpr size_t squash_rows_limit = 8192;
        if constexpr (is_streaming_reader)
            decoder_ptr = std::make_unique<CHBlockChunkDecodeAndSquash>(sample_block, squash_rows_limit);
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return name; }

    void cancel(bool kill) override
    {
        if (kill)
            remote_reader->cancel();
    }

    void readPrefixImpl() override
    {
        // for CoprocessorReader, we send Coprocessor requests in readPrefixImpl
        if constexpr (std::is_same_v<RemoteReader, CoprocessorReader>)
        {
            remote_reader->open();
        }
        // note that for ExchangeReceiver, we have sent EstablishMPPConnection requests before we construct the pipeline
    }

    Block readImpl() override
    {
        if (block_queue.empty())
        {
            if (!fetchRemoteResult())
                return {};
        }
        Block block = block_queue.front();
        block_queue.pop();
        return block;
    }

    const RemoteExecutionSummary & getRemoteExecutionSummary() { return remote_execution_summary; }

    size_t getTotalRows() const { return total_rows; }
    size_t getSourceNum() const { return source_num; }
    bool isStreamingCall() const { return is_streaming_reader; }
    const std::vector<ConnectionProfileInfo> & getConnectionProfileInfos() const { return connection_profile_infos; }

protected:
    void readSuffixImpl() override { LOG_DEBUG(log, "finish read {} rows from remote", total_rows); }

    void appendInfo(FmtBuffer & buffer) const override
    {
        buffer.append(": schema: {");
        buffer.joinStr(
            sample_block.begin(),
            sample_block.end(),
            [](const auto & arg, FmtBuffer & fb) { fb.fmtAppend("<{}, {}>", arg.name, arg.type->getName()); },
            ", ");
        buffer.append("}");
    }
};

using ExchangeReceiverInputStream = TiRemoteBlockInputStream<ExchangeReceiver>;
using CoprocessorBlockInputStream = TiRemoteBlockInputStream<CoprocessorReader>;
} // namespace DB
