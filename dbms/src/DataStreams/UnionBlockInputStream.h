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

#include <Common/MPMCQueue.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


namespace UnionBlockInputStreamImpl
{
template <StreamUnionMode mode>
struct OutputData;

/// A block or an exception.
template <>
struct OutputData<StreamUnionMode::Basic>
{
    Block block;
    std::exception_ptr exception;

    OutputData() = default;
    explicit OutputData(Block & block_)
        : block(block_)
    {}
    explicit OutputData(const std::exception_ptr & exception_)
        : exception(exception_)
    {}
};

/// Block + additional information or an exception.
template <>
struct OutputData<StreamUnionMode::ExtraInfo>
{
    Block block;
    BlockExtraInfo extra_info;
    std::exception_ptr exception;

    OutputData() = default;
    OutputData(Block & block_, BlockExtraInfo & extra_info_)
        : block(block_)
        , extra_info(extra_info_)
    {}
    explicit OutputData(const std::exception_ptr & exception_)
        : exception(exception_)
    {}
};

} // namespace UnionBlockInputStreamImpl

/** Merges several sources into one.
  * Blocks from different sources are interleaved with each other in an arbitrary way.
  * You can specify the number of threads (max_threads),
  *  in which data will be retrieved from different sources.
  *
  * It's managed like this:
  * - with the help of ParallelInputsProcessor in several threads it takes out blocks from the sources;
  * - the completed blocks are added to a limited queue of finished blocks;
  * - the main thread takes out completed blocks from the queue of finished blocks;
  * - if the StreamUnionMode::ExtraInfo mode is specified, in addition to the UnionBlockInputStream
  *   extracts blocks information; In this case all sources should support such mode.
  */

template <StreamUnionMode mode = StreamUnionMode::Basic, bool ignore_block = false>
class UnionBlockInputStream final : public IProfilingBlockInputStream
{
public:
    using ExceptionCallback = std::function<void()>;

private:
    using Self = UnionBlockInputStream<mode, ignore_block>;
    static constexpr auto NAME = "Union";
    using Payload = UnionBlockInputStreamImpl::OutputData<mode>;

public:
    UnionBlockInputStream(
        BlockInputStreams inputs,
        BlockInputStreams additional_inputs_at_end,
        size_t max_threads,
        Int64 max_buffered_bytes,
        const String & req_id,
        ExceptionCallback exception_callback_ = ExceptionCallback())
        : output_queue(
            CapacityLimits(
                std::min(std::max(inputs.size(), additional_inputs_at_end.size()), max_threads) * 5,
                max_buffered_bytes),
            [](const Payload & element) { return element.block.allocatedBytes(); }) // reduce contention
        , log(Logger::get(req_id))
        , handler(*this)
        , processor(inputs, additional_inputs_at_end, max_threads, handler, log)
        , exception_callback(exception_callback_)
    {
        // TODO: assert capacity of output_queue is not less than processor.getMaxThreads()
        children = inputs;
        children.insert(children.end(), additional_inputs_at_end.begin(), additional_inputs_at_end.end());

        size_t num_children = children.size();
        if (num_children > 1)
        {
            Block header = children.at(0)->getHeader();
            for (size_t i = 1; i < num_children; ++i)
                assertBlocksHaveEqualStructure(children[i]->getHeader(), header, "UNION");
        }
    }

    String getName() const override { return NAME; }

    ~UnionBlockInputStream() override
    {
        try
        {
            if (!all_read)
                cancel(false);

            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    /** Different from the default implementation by trying to stop all sources,
      * skipping failed by execution.
      */
    void cancel(bool kill) override
    {
        if (kill)
            is_killed = true;

        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
            return;

        processor.cancel(kill);
    }

    BlockExtraInfo getBlockExtraInfo() const override { return doGetBlockExtraInfo(); }

    Block getHeader() const override { return children.at(0)->getHeader(); }

    virtual void collectNewThreadCountOfThisLevel(int & cnt) override { cnt += processor.getMaxThreads(); }

protected:
    void finalize()
    {
        if (!started)
            return;

        LOG_TRACE(log, "Waiting for threads to finish");

        std::exception_ptr exception;
        if (!all_read)
        {
            /** Let's read everything up to the end, so that ParallelInputsProcessor is not blocked when trying to insert into the queue.
              * Maybe there is an exception in the queue.
              */
            UnionBlockInputStreamImpl::OutputData<mode> res;
            while (true)
            {
                output_queue.pop(res);

                if (res.exception)
                {
                    if (!exception)
                        exception = res.exception;
                    else if (Exception * e = exception_cast<Exception *>(exception))
                        e->addMessage("\n" + getExceptionMessage(res.exception, false));
                }
                else if (!res.block)
                    break;
            }

            all_read = true;
        }

        processor.wait();

        LOG_TRACE(log, "Waited for threads to finish");

        if (exception)
            std::rethrow_exception(exception);
    }

    /// Do nothing, to make the preparation for the query execution in parallel, in ParallelInputsProcessor.
    void readPrefix() override {}

    /** The following options are possible:
      * 1. `readImpl` function is called until it returns an empty block.
      *  Then `readSuffix` function is called and then destructor.
      * 2. `readImpl` function is called. At some point, `cancel` function is called perhaps from another thread.
      *  Then `readSuffix` function is called and then destructor.
      * 3. At any time, the object can be destroyed (destructor called).
      */

    Block readImpl() override
    {
        if (all_read)
            return received_payload.block;

        /// Run threads if this has not already been done.
        if (!started)
        {
            started = true;
            processor.process();
        }

        /// We will wait until the next block is ready or an exception is thrown.
        output_queue.pop(received_payload);

        if (received_payload.exception)
        {
            if (exception_callback)
                exception_callback();
            std::rethrow_exception(received_payload.exception);
        }

        if (!received_payload.block)
            all_read = true;

        return received_payload.block;
    }

    /// Called either after everything is read, or after cancel.
    void readSuffix() override
    {
        if (!all_read && !isCancelled())
            throw Exception("readSuffix called before all data is read", ErrorCodes::LOGICAL_ERROR);

        finalize();

        for (size_t i = 0; i < children.size(); ++i)
            children[i]->readSuffix();
    }

    uint64_t collectCPUTimeNsImpl(bool /*is_thread_runner*/) override
    {
        // `UnionBlockInputStream` does not count its own execute time,
        // whether `UnionBlockInputStream` is `thread-runner` or not,
        // because `UnionBlockInputStream` basically does not use cpu, only `condition_cv.wait`.
        uint64_t cpu_time_ns = 0;
        forEachChild([&](IBlockInputStream & child) {
            // Each of `UnionBlockInputStream`'s children is a thread-runner.
            cpu_time_ns += child.collectCPUTimeNs(true);
            return false;
        });
        return cpu_time_ns;
    }

private:
    BlockExtraInfo doGetBlockExtraInfo() const
    {
        if constexpr (mode == StreamUnionMode::ExtraInfo)
            return received_payload.extra_info;
        else
            throw Exception(
                "Method getBlockExtraInfo is not supported for mode StreamUnionMode::Basic",
                ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    using OutputQueue = MPMCQueue<Payload>;

private:
    /** The queue of the finished blocks. Also, you can put an exception instead of a block.
      * When data is run out, an empty block is inserted into the queue.
      * Sooner or later, an empty block is always inserted into the queue (even after exception or query cancellation).
      * The queue is always (even after exception or canceling the query, even in destructor) you must read up to an empty block,
      *  otherwise ParallelInputsProcessor can be blocked during insertion into the queue.
      */
    OutputQueue output_queue;
    std::mutex mu;
    bool meet_exception = false;

    void handleException(const std::exception_ptr & exception)
    {
        std::unique_lock lock(mu);
        if (meet_exception)
            return;
        meet_exception = true;
        /// The order of the rows matters. If it is changed, then the situation is possible,
        /// when before exception, an empty block (end of data) will be put into the queue,
        /// and the exception is lost.
        output_queue.emplace(exception);
        /// can not cancel itself or the exception might be lost
        /// use cancel instead of kill to avoid too many useless error message
        processor.cancel(false);
    }

    struct Handler
    {
        explicit Handler(Self & parent_)
            : parent(parent_)
        {}

        void onBlock(Block & block, size_t /*thread_num*/)
        {
            if constexpr (!ignore_block)
                parent.output_queue.emplace(block);
        }

        void onBlock(Block & block, BlockExtraInfo & extra_info, size_t /*thread_num*/)
        {
            if constexpr (!ignore_block)
                parent.output_queue.emplace(block, extra_info);
        }

        void onFinish() { parent.output_queue.emplace(); }

        void onFinishThread(size_t /*thread_num*/) {}

        void onException(std::exception_ptr & exception, size_t /*thread_num*/) { parent.handleException(exception); }

        String getName() const { return "ParallelUnion"; }

        Self & parent;
    };

    LoggerPtr log;

    Handler handler;
    ParallelInputsProcessor<Handler, mode> processor;

    ExceptionCallback exception_callback;

    Payload received_payload;

    bool started = false;
    bool all_read = false;
};

} // namespace DB
