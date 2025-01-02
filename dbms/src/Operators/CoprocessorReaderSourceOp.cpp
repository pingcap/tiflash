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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Operators/CoprocessorReaderSourceOp.h>

namespace DB
{
CoprocessorReaderSourceOp::CoprocessorReaderSourceOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    CoprocessorReaderPtr coprocessor_reader_)
    : SourceOp(exec_context_, req_id)
    , coprocessor_reader(coprocessor_reader_)
    , io_profile_info(IOProfileInfo::createForRemote(
          profile_info_ptr,
          coprocessor_reader->getSourceNum(),
          CoprocessorReader::conn_type_vec))
{
    assert(coprocessor_reader);
    setHeader(Block(getColumnWithTypeAndName(toNamesAndTypes(coprocessor_reader->getOutputSchema()))));
}

String CoprocessorReaderSourceOp::getName() const
{
    return "CoprocessorReaderSourceOp";
}

void CoprocessorReaderSourceOp::operatePrefixImpl()
{
    LOG_DEBUG(log, "start reading from remote coprocessor", total_rows);
    coprocessor_reader->open();
}

void CoprocessorReaderSourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish read {} rows from remote coprocessor", total_rows);
}

Block CoprocessorReaderSourceOp::popFromBlockQueue()
{
    assert(!block_queue.empty());
    Block block = std::move(block_queue.front());
    block_queue.pop();
    return block;
}

OperatorStatus CoprocessorReaderSourceOp::readImpl(Block & block)
{
    if (!block_queue.empty())
    {
        block = popFromBlockQueue();
        return OperatorStatus::HAS_OUTPUT;
    }

    while (true)
    {
        assert(block_queue.empty());
        auto await_status = awaitImpl();
        if (await_status == OperatorStatus::HAS_OUTPUT)
        {
            assert(reader_res);
            assert(reader_res->second || reader_res->first.finished);
            auto result = coprocessor_reader->toResult(*reader_res, block_queue, header);
            reader_res.reset();

            if (result.meet_error)
            {
                LOG_WARNING(log, "coprocessor reader meets error: {}", result.error_msg);
                throw Exception(result.error_msg);
            }
            if (result.resp != nullptr && result.resp->has_error())
            {
                LOG_WARNING(log, "coprocessor reader meets error: {}", result.resp->error().DebugString());
                throw Exception(result.resp->error().DebugString());
            }
            if (result.eof)
            {
                LOG_DEBUG(log, "coprocessor reader meets eof");
                return OperatorStatus::HAS_OUTPUT;
            }

            const auto & decode_detail = result.decode_detail;
            if (result.same_zone_flag)
            {
                io_profile_info->connection_profile_infos[0].packets += decode_detail.packets;
                io_profile_info->connection_profile_infos[0].bytes += decode_detail.packet_bytes;
            }
            else
            {
                io_profile_info->connection_profile_infos[1].packets += decode_detail.packets;
                io_profile_info->connection_profile_infos[1].bytes += decode_detail.packet_bytes;
            }

            total_rows += decode_detail.rows;
            LOG_TRACE(
                log,
                "recv {} rows from corprocessor reeader for {}, total recv row num: {}",
                decode_detail.rows,
                result.req_info,
                total_rows);

            if (decode_detail.rows <= 0)
                continue;

            block = popFromBlockQueue();
            return OperatorStatus::HAS_OUTPUT;
        }
        assert(!reader_res);
        return await_status;
    }
}
OperatorStatus CoprocessorReaderSourceOp::awaitImpl()
{
    if unlikely (!block_queue.empty())
        return OperatorStatus::HAS_OUTPUT;
    if unlikely (reader_res)
        return OperatorStatus::HAS_OUTPUT;

    reader_res.emplace(coprocessor_reader->nonBlockingNext());
    if (reader_res->second || reader_res->first.finished)
    {
        return OperatorStatus::HAS_OUTPUT;
    }
    else
    {
        reader_res.reset();
        return OperatorStatus::WAITING;
    }
}
} // namespace DB
