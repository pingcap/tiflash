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

namespace DB
{
class ParallelWriter
{
public:
    ParallelWriter() = default;

    virtual void onBlock(Block & block, size_t thread_num) = 0;
    virtual void onFinishThread(size_t thread_num) = 0;
    virtual void onFinish() = 0;

    virtual ~ParallelWriter() = default;
};
using ParallelWriterPtr = std::shared_ptr<ParallelWriter>;

class ParallelBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "Parallel";

public:
    ParallelBlockInputStream(
        const BlockInputStreams & inputs,
        const BlockInputStreamPtr & additional_input_at_end,
        size_t max_threads_,
        const ParallelWriterPtr & parallel_writer_,
        const String & req_id);

    String getName() const override { return NAME; }

    void cancel(bool kill) override;

    Block getHeader() const override;

    virtual void collectNewThreadCountOfThisLevel(int & cnt) override
    {
        cnt += processor.getMaxThreads();
    }

protected:
    /// Do nothing that preparation to execution of the query be done in parallel, in ParallelInputsProcessor.
    void readPrefix() override
    {
    }

    Block readImpl() override;

private:
    const LoggerPtr log;

    size_t max_threads;

    ParallelWriterPtr parallel_writer;

    std::atomic<bool> executed{false};

    Exceptions exceptions;
    std::atomic<Int32> first_exception_index{-1};

    struct Handler
    {
        explicit Handler(ParallelBlockInputStream & parent_)
            : parent(parent_)
        {}

        void onBlock(Block & block, size_t thread_num);
        void onFinishThread(size_t thread_num);
        void onFinish();
        void onException(std::exception_ptr & exception, size_t thread_num);
        static String getName()
        {
            return "ParallelWriter";
        }

        ParallelBlockInputStream & parent;
    };

    Handler handler;
    ParallelInputsProcessor<Handler> processor;
};

} // namespace DB
