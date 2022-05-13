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

#include <DataStreams/ParallelWritingBlockInputStream.h>

namespace DB
{
SerialWritingBlockInputStream::SerialWritingBlockInputStream(
    const BlockInputStreamPtr & input,
    const BlockInputStreamPtr & additional_input_at_end,
    const ParallelWriterPtr & parallel_writer_,
    const String & req_id)
    : log(Logger::get(NAME, req_id))
    , parallel_writer(parallel_writer_)
{
    children.push_back(input);
    if (additional_input_at_end)
        children.push_back(additional_input_at_end);
}

Block SerialWritingBlockInputStream::getHeader() const
{
    return children.back()->getHeader();
}

Block SerialWritingBlockInputStream::readImpl()
{
    for (const auto & child : children)
    {
        while (Block block = child->read())
        {
            parallel_writer->write(block, 0);
        }
    }

    parallel_writer->onFinishThread(0);
    parallel_writer->onFinish();

    return {};
}

ParallelWritingBlockInputStream::ParallelWritingBlockInputStream(
    const BlockInputStreams & inputs,
    const BlockInputStreamPtr & additional_input_at_end,
    const ParallelWriterPtr & parallel_writer_,
    size_t max_threads_,
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


Block ParallelWritingBlockInputStream::getHeader() const
{
    return children.back()->getHeader();
}


void ParallelWritingBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    if (!executed)
        processor.cancel(kill);
}


Block ParallelWritingBlockInputStream::readImpl()
{
    if (!executed)
    {
        exceptions.resize(max_threads);

        LOG_FMT_TRACE(log, "ParallelWriting");

        processor.process();
        processor.wait();

        if (first_exception_index != -1)
            std::rethrow_exception(exceptions[first_exception_index]);

        if (isCancelledOrThrowIfKilled())
            return {};

        executed = true;
    }

    return {};
}

void ParallelWritingBlockInputStream::Handler::onBlock(Block & block, size_t thread_num)
{
    parent.parallel_writer->write(block, thread_num);
}

void ParallelWritingBlockInputStream::Handler::onFinishThread(size_t thread_num)
{
    if (!parent.isCancelled())
    {
        parent.parallel_writer->onFinishThread(thread_num);
    }
}

void ParallelWritingBlockInputStream::Handler::onFinish()
{
    if (!parent.isCancelled())
    {
        parent.parallel_writer->onFinish();
    }
}

void ParallelWritingBlockInputStream::Handler::onException(std::exception_ptr & exception, size_t thread_num)
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
