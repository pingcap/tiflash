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
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/DecodeDetail.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <common/logger_useful.h>

#include <chrono>
#include <mutex>
#include <thread>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <kvproto/mpp.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Rpc.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop


namespace DB
{
struct CoprocessorReaderResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    bool meet_error;
    String error_msg;
    bool eof;
    String req_info = "cop request";
    DecodeDetail decode_detail;

    CoprocessorReaderResult(
        std::shared_ptr<tipb::SelectResponse> resp_,
        bool meet_error_ = false,
        const String & error_msg_ = "",
        bool eof_ = false,
        DecodeDetail decode_detail_ = {})
        : resp(resp_)
        , meet_error(meet_error_)
        , error_msg(error_msg_)
        , eof(eof_)
        , decode_detail(decode_detail_)
    {}
};

/// this is an adapter for pingcap::coprocessor::ResponseIter, so it can be used in TiRemoteBlockInputStream
class CoprocessorReader
{
public:
    static constexpr bool is_streaming_reader = false;
    static constexpr auto name = "CoprocessorReader";

private:
    DAGSchema schema;
    bool has_enforce_encode_type;
    pingcap::coprocessor::ResponseIter resp_iter;

public:
    CoprocessorReader(
        const DAGSchema & schema_,
        pingcap::kv::Cluster * cluster,
        std::vector<pingcap::coprocessor::CopTask> tasks,
        bool has_enforce_encode_type_,
        int concurrency,
        const pingcap::kv::LabelFilter & tiflash_label_filter_)
        : schema(schema_)
        , has_enforce_encode_type(has_enforce_encode_type_)
        , resp_iter(std::move(tasks), cluster, concurrency, &Poco::Logger::get("pingcap/coprocessor"), tiflash_label_filter_)
        , collected(false)
        , concurrency_(concurrency)
    {}

    const DAGSchema & getOutputSchema() const { return schema; }

    // `open` will call the resp_iter's `open` to send coprocessor request.
    void open()
    {
        resp_iter.open();
        opened = true;
    }

    // `cancel` will call the resp_iter's `cancel` to abort the data receiving and prevent the next retry.
    void cancel() { resp_iter.cancel(); }


    static DecodeDetail decodeChunks(
        const std::shared_ptr<tipb::SelectResponse> & resp,
        std::queue<Block> & block_queue,
        const Block & header,
        const DAGSchema & schema)
    {
        DecodeDetail detail;
        int chunk_size = resp->chunks_size();
        if (chunk_size == 0)
            return detail;

        detail.packet_bytes = resp->ByteSizeLong();
        for (int i = 0; i < chunk_size; i++)
        {
            Block block;
            const tipb::Chunk & chunk = resp->chunks(i);
            switch (resp->encode_type())
            {
            case tipb::EncodeType::TypeCHBlock:
                block = CHBlockChunkCodec::decode(chunk.rows_data(), header);
                break;
            case tipb::EncodeType::TypeChunk:
                block = ArrowChunkCodec().decode(chunk.rows_data(), schema);
                break;
            case tipb::EncodeType::TypeDefault:
                block = DefaultChunkCodec().decode(chunk.rows_data(), schema);
                break;
            default:
                throw Exception("Unsupported encode type", ErrorCodes::LOGICAL_ERROR);
            }

            detail.rows += block.rows();

            if (unlikely(block.rows() == 0))
                continue;
            /// CHBlockChunkCodec::decode already checked the schema.
            if (resp->encode_type() != tipb::EncodeType::TypeCHBlock)
                assertBlockSchema(header, block, "CoprocessorReader decode chunks");
            block_queue.push(std::move(block));
        }
        return detail;
    }

    // stream_id, decoder_ptr are only meaningful for ExchagneReceiver.
    CoprocessorReaderResult nextResult(std::queue<Block> & block_queue, const Block & header, size_t /*stream_id*/, std::unique_ptr<CHBlockChunkDecodeAndSquash> & /*decoder_ptr*/)
    {
        RUNTIME_CHECK(opened == true);

        auto && [result, has_next] = resp_iter.next();
        if (!result.error.empty())
            return {nullptr, true, result.error.message(), false};

        if (!has_next)
            return {nullptr, false, "", true};

        auto resp = std::make_shared<tipb::SelectResponse>();
        if (resp->ParseFromString(result.data()))
        {
            if (resp->has_error())
            {
                return {nullptr, true, resp->error().DebugString(), false};
            }
            else if (has_enforce_encode_type && resp->encode_type() != tipb::EncodeType::TypeCHBlock && resp->chunks_size() > 0)
            {
                return {nullptr,
                        true,
                        "Encode type of coprocessor response is not CHBlock, "
                        "maybe the version of some TiFlash node in the cluster is not match with this one",
                        false};
            }
            auto detail = decodeChunks(resp, block_queue, header, schema);
            return {resp, false, "", false, detail};
        }
        else
        {
            return {nullptr, true, "Error while decoding coprocessor::Response", false};
        }
    }

    size_t getSourceNum() const { return 1; }

    int getExternalThreadCnt() const { return concurrency_; }

    void close() {}

    bool collected = false;
    int concurrency_;
    bool opened = false;
};
} // namespace DB
