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
#include <Common/Stopwatch.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Storages/Columnar/ColumnarReadSourceOp.h>
#include <common/logger_useful.h>

namespace DB
{

void ColumnarReadSourceOp::operateSuffixImpl()
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
        "rows={} rows_per_sec={} bytes={} bytes_per_sec={} read_cost={:.3f}s",
        keyspace_id,
        total_cost_sec,
        total_streams,
        total_rows,
        rows_per_sec,
        total_bytes,
        bytes_per_sec,
        duration_read_sec);
}

void ColumnarReadSourceOp::operatePrefixImpl()
{
    total_cost_watch.restart();
    LOG_INFO(log, "Begin reading columnar snapshots, keyspace_id={}", exec_context.getKeyspaceID());
}

OperatorStatus ColumnarReadSourceOp::readImpl(Block & block)
{
    switch (state)
    {
    case ColumnarReadSourceState::DONE:
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarReadSourceState::READY_BLOCK:
        assert(t_block.has_value());
        std::swap(block, t_block.value());
        t_block.reset();
        state = ColumnarReadSourceState::READING;
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarReadSourceState::NEED_READER:
    case ColumnarReadSourceState::WAIT_READER:
    case ColumnarReadSourceState::READING:
        break; // hand off to awaitImpl
    }

    return awaitImpl();
}

void ColumnarReadSourceOp::consumeReadyReader(ColumnarReaderPtr reader)
{
    assert(current_reader_work);
    current_input_stream = RNColumnarInputStream::createWithReader(
        {
            .context = context,
            .log = log,
            .task = task,
            .reader_work = current_reader_work,
            .columns_to_read = task->getColumnsToRead(),
            .extra_table_id_index = task->getExtraTableIDIndex(),
            .table_id = task->getLogicalTableID(),
            .executor_id = task->getExecutorID(),
        },
        std::move(reader));
    current_reader_work.reset();
    ++total_streams;
    state = ColumnarReadSourceState::READING;
}

OperatorStatus ColumnarReadSourceOp::awaitImpl()
{
    switch (state)
    {
    case ColumnarReadSourceState::DONE:
    case ColumnarReadSourceState::READY_BLOCK:
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarReadSourceState::READING:
        // Have an input stream ready; read one block in executeIOImpl.
        return OperatorStatus::IO_IN;
    case ColumnarReadSourceState::WAIT_READER:
    {
        assert(current_reader_work);
        const auto region_id = current_reader_work->plan.region_id;
        std::optional<ColumnarReaderPtr> taken_reader;
        {
            std::lock_guard lock(current_reader_work->mutex);
            switch (current_reader_work->state)
            {
            case RNColumnarReaderMaterializeState::Ready:
                taken_reader.emplace(std::move(current_reader_work->reader.value()));
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
                break;
            case RNColumnarReaderMaterializeState::Failed:
                std::rethrow_exception(current_reader_work->exception);
            case RNColumnarReaderMaterializeState::Consumed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "columnar reader work for region {} is already consumed",
                    region_id);
            case RNColumnarReaderMaterializeState::Creating:
                // Do not wait on the one-shot prefetch notification; enter IO pool and materialize inline.
                return OperatorStatus::IO_IN;
            case RNColumnarReaderMaterializeState::NotStarted:
                current_reader_work->state = RNColumnarReaderMaterializeState::Creating;
                return OperatorStatus::IO_IN;
            }
        }
        if (taken_reader.has_value())
        {
            consumeReadyReader(std::move(taken_reader.value()));
            return OperatorStatus::IO_IN;
        }
        return OperatorStatus::IO_IN; // unreachable
    }
    case ColumnarReadSourceState::NEED_READER:
    {
        // Prefetch is now executed via TaskScheduler-submitted PrefetchColumnarReaderTask
        // (IO pool) instead of detached threads, so it is safe to enable here.
        auto next_work = task->tryAcquireReaderWork(/*enable_prefetch=*/true);
        if (!next_work.has_value())
        {
            state = ColumnarReadSourceState::DONE;
            return OperatorStatus::HAS_OUTPUT;
        }
        current_reader_work = std::move(next_work.value());
        const auto region_id = current_reader_work->plan.region_id;

        std::optional<ColumnarReaderPtr> taken_reader;
        bool should_materialize = false;
        {
            std::lock_guard lock(current_reader_work->mutex);
            switch (current_reader_work->state)
            {
            case RNColumnarReaderMaterializeState::Ready:
                taken_reader.emplace(std::move(current_reader_work->reader.value()));
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
                break;
            case RNColumnarReaderMaterializeState::Failed:
                std::rethrow_exception(current_reader_work->exception);
            case RNColumnarReaderMaterializeState::Consumed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "columnar reader work for region {} is already consumed",
                    region_id);
            case RNColumnarReaderMaterializeState::NotStarted:
            case RNColumnarReaderMaterializeState::Creating:
                current_reader_work->state = RNColumnarReaderMaterializeState::Creating;
                should_materialize = true;
                break;
            }
        }

        if (taken_reader.has_value())
        {
            consumeReadyReader(std::move(taken_reader.value()));
            return OperatorStatus::IO_IN;
        }
        if (should_materialize)
            return OperatorStatus::IO_IN;
        return OperatorStatus::IO_IN; // unreachable
    }
    }

    return OperatorStatus::IO_IN; // unreachable
}

OperatorStatus ColumnarReadSourceOp::executeIOImpl()
{
    switch (state)
    {
    case ColumnarReadSourceState::DONE:
    case ColumnarReadSourceState::READY_BLOCK:
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarReadSourceState::NEED_READER:
    case ColumnarReadSourceState::WAIT_READER:
    {
        assert(current_reader_work);
        std::optional<ColumnarReaderPtr> taken_reader;
        {
            std::lock_guard lock(current_reader_work->mutex);
            if (current_reader_work->state == RNColumnarReaderMaterializeState::Ready)
            {
                taken_reader.emplace(std::move(current_reader_work->reader.value()));
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
            }
        }

        if (taken_reader.has_value())
        {
            consumeReadyReader(std::move(taken_reader.value()));
        }
        else
        {
            auto reader = task->createColumnarReaderWithBackoff(current_reader_work);
            {
                std::lock_guard lock(current_reader_work->mutex);
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
            }
            current_reader_work->cv.notify_all();
            current_reader_work->notify_future.notifyAll();
            consumeReadyReader(std::move(reader));
        }
        // fall through to READING
    }
    case ColumnarReadSourceState::READING:
    {
        assert(current_input_stream);
        FilterPtr filter_ignored = nullptr;
        Stopwatch w{CLOCK_MONOTONIC_COARSE};
        Block block = current_input_stream->read(filter_ignored, false);
        duration_read_sec += w.elapsedSeconds();
        if likely (block && block.rows() > 0)
        {
            total_rows += block.rows();
            total_bytes += block.bytes();
            t_block.emplace(std::move(block));
            state = ColumnarReadSourceState::READY_BLOCK;
            return OperatorStatus::HAS_OUTPUT;
        }

        current_input_stream.reset();
        state = ColumnarReadSourceState::NEED_READER;
        return awaitImpl();
    }
    }

    return OperatorStatus::HAS_OUTPUT; // unreachable
}

} // namespace DB
#endif
