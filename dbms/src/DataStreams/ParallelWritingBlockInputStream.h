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
#include <DataStreams/ParallelInputsProcessor.h>
#include <DataStreams/ParallelWriter.h>

namespace DB
{
BlockInputStreamPtr executeParallelWrite(
    const BlockInputStreams & inputs,
    const BlockInputStreamPtr & additional_input_at_end,
    const ParallelWriterPtr & parallel_writer,
    size_t max_threads,
    const String & req_id);

class SerialWritingBlockInputStream : public IProfilingBlockInputStream
{
public:
    SerialWritingBlockInputStream(
        const BlockInputStreamPtr & input,
        const BlockInputStreamPtr & additional_input_at_end,
        const ParallelWriterPtr & parallel_writer_,
        const String & req_id);

    String getName() const override { return name; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

    void appendInfo(FmtBuffer & buffer) const override;

private:
    const String name;

    const LoggerPtr log;

    ParallelWriterPtr parallel_writer;
};

class ParallelWritingBlockInputStream : public IProfilingBlockInputStream
{
public:
    ParallelWritingBlockInputStream(
        const BlockInputStreams & inputs,
        const BlockInputStreamPtr & additional_input_at_end,
        const ParallelWriterPtr & parallel_writer_,
        size_t max_threads_,
        const String & req_id);

    String getName() const override { return name; }

    void cancel(bool kill) override;

    Block getHeader() const override;

    void collectNewThreadCountOfThisLevel(int & cnt) override
    {
        cnt += processor.getMaxThreads();
    }

protected:
    /// Do nothing that preparation to execution of the query be done in parallel, in ParallelInputsProcessor.
    void readPrefix() override
    {
    }

    Block readImpl() override;

    void appendInfo(FmtBuffer & buffer) const override;

private:
    const String name;

    const LoggerPtr log;

    size_t max_threads;

    std::atomic<bool> executed{false};

    Exceptions exceptions;
    std::atomic<Int32> first_exception_index{-1};

    struct Handler
    {
        explicit Handler(ParallelWritingBlockInputStream & parent_)
            : parent(parent_)
        {}

        void onBlock(Block & block, size_t thread_num);
        void onFinishThread(size_t thread_num);
        void onFinish();
        void onException(std::exception_ptr & exception, size_t thread_num);
        static String getName()
        {
            return "ParallelWriting";
        }

        ParallelWritingBlockInputStream & parent;
    };

    ParallelWriterPtr parallel_writer;

    Handler handler;
    ParallelInputsProcessor<Handler> processor;
};
} // namespace DB
