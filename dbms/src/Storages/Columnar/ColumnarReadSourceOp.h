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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <Operators/Operator.h>
#include <Storages/StorageDisaggregatedColumnar.h>

#include <optional>

namespace DB
{

/// Explicit state machine for ColumnarReadSourceOp.
/// It owns reader works and emits deserialized blocks to a downstream SharedQueueSinkOp.
enum class ColumnarReadSourceState : uint8_t
{
    NEED_READER, // No current reader work; awaitImpl will acquire one.
    WAIT_READER, // Compatibility state for a reader work that is Creating; pipeline still returns IO_IN.
    READING, // Reader is ready and input stream created; ready to read a block.
    READY_BLOCK, // t_block has a cached block for downstream.
    DONE, // All reader works consumed.
};

class ColumnarReadSourceOp : public SourceOp
{
    static constexpr auto NAME = "RNProxy";

public:
    struct Options
    {
        PipelineExecutorContext & exec_context;
        RNColumnarReadTaskPtr task;
    };

    explicit ColumnarReadSourceOp(const Options & options)
        : SourceOp(options.exec_context, options.task->getLog()->identifier())
        , context(options.task->getContext())
        , log(options.task->getLog())
        , task(options.task)
    {
        setHeader(AddExtraTableIDColumnTransformAction::buildHeader(
            options.task->getColumnsToRead(),
            options.task->getExtraTableIDIndex()));
    }

    static SourceOpPtr create(const Options & options) { return std::make_unique<ColumnarReadSourceOp>(options); }

    String getName() const override { return NAME; }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    void operateSuffixImpl() override;

    void operatePrefixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

    OperatorStatus executeIOImpl() override;

private:
    /// Create an input stream from an already-materialized reader, then transition to READING.
    void consumeReadyReader(ColumnarReaderPtr reader);

    const Context & context;
    const LoggerPtr log;
    RNColumnarReadTaskPtr task;
    UInt64 total_bytes = 0;
    size_t total_rows = 0;
    size_t total_streams = 0;

    BlockInputStreamPtr current_input_stream;

    // IO work caches one block here so the next CPU-side readImpl can push it into SharedQueueSinkOp.
    std::optional<Block> t_block = std::nullopt;

    // The reader work currently being consumed by this producer source.
    RNColumnarReaderWorkPtr current_reader_work;

    ColumnarReadSourceState state = ColumnarReadSourceState::NEED_READER;
    Stopwatch total_cost_watch{CLOCK_MONOTONIC_COARSE};

    // Count the time consumed by reading blocks in the stream of reader works.
    double duration_read_sec = 0;
};

} // namespace DB
#endif
