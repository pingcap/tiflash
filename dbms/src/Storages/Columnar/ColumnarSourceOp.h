// Copyright 2026 PingCAP, Inc.
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
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context_fwd.h>
#include <Operators/Operator.h>
#include <Storages/Columnar/ColumnarReader.h>

namespace DB
{

class ColumnarSourceOp : public SourceOp
{
    static constexpr auto NAME = "ColumnarSource";

public:
    struct Options
    {
        PipelineExecutorContext & exec_context;
        ColumnarReadTaskPoolPtr task;
    };

    explicit ColumnarSourceOp(const Options & options)
        : SourceOp(options.exec_context, options.task->getLog()->identifier())
        , context(options.task->getContext())
        , log(options.task->getLog())
        , task(options.task)
    {
        setHeader(AddExtraTableIDColumnTransformAction::buildHeader(
            options.task->getColumnsToRead(),
            options.task->getExtraTableIDIndex()));
    }

    static SourceOpPtr create(const Options & options) { return std::make_unique<ColumnarSourceOp>(options); }

    String getName() const override { return NAME; }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    void operateSuffixImpl() override;

    void operatePrefixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

    OperatorStatus executeIOImpl() override;

private:
    const Context & context;
    const LoggerPtr log;
    ColumnarReadTaskPoolPtr task;
    UInt64 total_bytes = 0;
    size_t total_rows = 0;
    size_t total_streams = 0;

    BlockInputStreamPtr current_input_stream;

    // Temporarily store the block read from current_seg_task->stream and pass it to downstream operators in readImpl.
    std::optional<Block> t_block = std::nullopt;

    bool done = false;

    Stopwatch total_cost_watch{CLOCK_MONOTONIC_COARSE};

    // Count the time consumed by reading blocks in the stream of segment tasks.
    double duration_read_sec = 0;
};

} // namespace DB
#endif
