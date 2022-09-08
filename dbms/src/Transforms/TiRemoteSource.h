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

#include <Common/FmtUtils.h>
#include <Transforms/Source.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

namespace DB
{
// TiRemoteSource is a source that read/receive data from remote.
template <typename RemoteReader>
class TiRemoteSource : public Source
{
    static constexpr bool is_streaming_reader = RemoteReader::is_streaming_reader;

    std::shared_ptr<RemoteReader> remote_reader;
    size_t source_num;

    Block sample_block;

    std::queue<Block> block_queue;

    String name;

    const LoggerPtr log;

    uint64_t total_rows;

    // For fine grained shuffle, sender will partition data into muiltiple streams by hashing.
    // ExchangeReceiverBlockInputStream only need to read its own stream, i.e., streams[stream_id].
    // CoprocessorBlockInputStream doesn't take care of this.
    size_t stream_id;

    enum class FetchResult
    {
        finished, fetched, notFetched
    };
    FetchResult fetchRemoteResult()
    {
        auto result = remote_reader->nextResult(block_queue, sample_block, stream_id);
        if (result.meet_error)
        {
            LOG_FMT_WARNING(log, "remote reader meets error: {}", result.error_msg);
            throw Exception(result.error_msg);
        }
        if (result.eof)
            return FetchResult::finished;
        if (result.await)
            return FetchResult::notFetched;
        if (result.resp != nullptr && result.resp->has_error())
        {
            LOG_FMT_WARNING(log, "remote reader meets error: {}", result.resp->error().DebugString());
            throw Exception(result.resp->error().DebugString());
        }

        const auto & decode_detail = result.decode_detail;

        total_rows += decode_detail.rows;
        LOG_FMT_TRACE(
            log,
            "recv {} rows from remote for {}, total recv row num: {}",
            decode_detail.rows,
            result.req_info,
            total_rows);
        if (decode_detail.rows == 0)
            return fetchRemoteResult();
        return FetchResult::fetched;
    }

public:
    TiRemoteSource(std::shared_ptr<RemoteReader> remote_reader_, const String & req_id, const String & executor_id, size_t stream_id_)
        : remote_reader(remote_reader_)
        , source_num(remote_reader->getSourceNum())
        , name(fmt::format("TiRemote({})", RemoteReader::name))
        , log(Logger::get(name, req_id, executor_id))
        , total_rows(0)
        , stream_id(stream_id_)
    {
        sample_block = Block(getColumnWithTypeAndName(toNamesAndTypes(remote_reader->getOutputSchema())));
    }

    Block getHeader() const override { return sample_block; }

    void cancel(bool kill) override
    {
        if (kill)
            remote_reader->cancel();
    }
    std::pair<bool, Block> read() override
    {
        if (block_queue.empty())
        {
            auto fetch_result = fetchRemoteResult();
            switch (fetch_result)
            {
            case FetchResult::finished:
                return {true, {}};
            case FetchResult::notFetched:
                return {false, {}};
            default:
                ;
            }
        }
        // todo should merge some blocks to make sure the output block is big enough
        Block block = block_queue.front();
        block_queue.pop();
        return {true, std::move(block)};
    }
};

using ExchangeReceiverSource = TiRemoteSource<ExchangeReceiver>;
// using CoprocessorBlockInputStream = TiRemoteBlockInputStream<CoprocessorReader>;
} // namespace DB
