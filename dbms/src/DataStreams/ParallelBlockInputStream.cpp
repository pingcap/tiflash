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

#include <DataStreams/ParallelBlockInputStream.h>

namespace DB
{
ParallelBlockInputStream::ParallelBlockInputStream(
    const BlockInputStreams & inputs,
    const BlockInputStreamPtr & additional_input_at_end,
    size_t max_threads_,
    const ParallelWriterPtr & parallel_writer_,
    const String & req_id)
    : log(Logger::get(NAME, req_id))
    , max_threads(std::min(inputs.size(), max_threads_))
    , parallel_writer(parallel_writer_)
    , handler(*this)
    , processor(inputs, additional_input_at_end, max_threads, handler, log)
{
    children = inputs;
    if (additional_input_at_end)
        children.push_back(additional_input_at_end);
}

Block ParallelBlockInputStream<StreamHandler>::getHeader() const
{
    return children.back()->getHeader();
}

void ParallelBlockInputStream<StreamHandler>::cancel(bool kill)
{
    if (kill)
        is_killed = true;
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    if (!executed)
        processor.cancel(kill);
}

Block ParallelBlockInputStream<StreamHandler>::readImpl()
{
    if (!executed)
    {
        exceptions.resize(max_threads);

        processor.process();
        processor.wait();

        if (first_exception_index != -1)
            std::rethrow_exception(exceptions[first_exception_index]);

        if (isCancelledOrThrowIfKilled())
            return {};

        executed = true;
    }

    isCancelledOrThrowIfKilled();
    return {};
}

void ParallelBlockInputStream<StreamHandler>::Handler::onBlock(Block & block, size_t thread_num)
{
    parent.parallel_writer->onBlock(block, thread_num);
}

void ParallelBlockInputStream<StreamHandler>::Handler::onFinishThread(size_t thread_num)
{
    if (!parent.isCancelled())
    {
        parent.parallel_writer->onFinishThread(thread_num);
    }
}

void ParallelBlockInputStream<StreamHandler>::Handler::onFinish()
{
    if (!parent.isCancelled())
    {
        parent.parallel_writer->onFinish();
    }
}

void ParallelBlockInputStream<StreamHandler>::Handler::onException(std::exception_ptr & exception, size_t thread_num)
{
    parent.exceptions[thread_num] = exception;
    Int32 old_value = -1;
    parent.first_exception_index.compare_exchange_strong(old_value, static_cast<Int32>(thread_num), std::memory_order_seq_cst, std::memory_order_relaxed);

    /// can not cancel parent inputStream or the exception might be lost
    if (!parent.executed)
        /// kill the processor so ExchangeReceiver will be closed
        parent.processor.cancel(true);
}
} // namespace DB
