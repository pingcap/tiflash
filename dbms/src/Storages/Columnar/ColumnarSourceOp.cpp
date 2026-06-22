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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/RedactHelpers.h>
#include <Common/Stopwatch.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/Columnar/ColumnarSourceOp.h>

namespace DB
{

void ColumnarSourceOp::operateSuffixImpl()
{
    UNUSED(context);
    const auto keyspace_id = exec_context.getKeyspaceID();
    const double total_cost_sec = total_cost_watch.elapsedSeconds();
    const UInt64 rows_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_rows) / total_cost_sec) : 0;
    const UInt64 bytes_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_bytes) / total_cost_sec) : 0;
    LOG_INFO(
        log,
        "Finished reading columnar snapshots, keyspace_id={} task_pool_worker_total_cost={:.3f}s claimed_streams={} "
        "rows={} "
        "rows_per_sec={} "
        "bytes={} bytes_per_sec={} read_cost={:.3f}s",
        keyspace_id,
        total_cost_sec,
        total_streams,
        total_rows,
        rows_per_sec,
        total_bytes,
        bytes_per_sec,
        duration_read_sec);
}

void ColumnarSourceOp::operatePrefixImpl()
{
    total_cost_watch.restart();
    LOG_INFO(log, "Begin reading columnar snapshots, keyspace_id={}", exec_context.getKeyspaceID());
}

OperatorStatus ColumnarSourceOp::readImpl(Block & block)
{
    if (unlikely(done))
    {
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    }

    if (t_block.has_value())
    {
        std::swap(block, t_block.value());
        t_block.reset();
        return OperatorStatus::HAS_OUTPUT;
    }

    return awaitImpl();
}

OperatorStatus ColumnarSourceOp::awaitImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    return OperatorStatus::IO_IN;
}

OperatorStatus ColumnarSourceOp::executeIOImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    if (!current_input_stream)
    {
        auto next_reader_work = task->tryAcquireReaderWork();
        if (!next_reader_work.has_value())
        {
            done = true;
            return OperatorStatus::HAS_OUTPUT;
        }
        current_input_stream = task->createInputStream(next_reader_work.value());
        ++total_streams;
    }

    FilterPtr filter_ignored = nullptr;
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    Block block = current_input_stream->read(filter_ignored, false);
    duration_read_sec += w.elapsedSeconds();
    if likely (block && block.rows() > 0)
    {
        total_rows += block.rows();
        total_bytes += block.bytes();
        t_block.emplace(std::move(block));
        return OperatorStatus::HAS_OUTPUT;
    }
    else
    {
        current_input_stream.reset();
        return awaitImpl();
    }
}

} // namespace DB
#endif
