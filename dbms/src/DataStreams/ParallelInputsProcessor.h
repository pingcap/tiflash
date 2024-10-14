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

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <common/logger_useful.h>

#include <atomic>
#include <list>
#include <mutex>
#include <queue>
#include <thread>


/** Allows to process multiple block input streams (sources) in parallel, using specified number of threads.
  * Reads (pulls) blocks from any available source and passes it to specified handler.
  *
  * Before any reading, calls "readPrefix" method of sources in parallel.
  *
  * (As an example, "readPrefix" can prepare connections to remote servers,
  *  and we want this work to be executed in parallel for different sources)
  *
  * Implemented in following way:
  * - there are multiple input sources to read blocks from;
  * - there are multiple threads, that could simultaneously read blocks from different sources;
  * - "available" sources (that are not read in any thread right now) are put in queue of sources;
  * - when thread take a source to read from, it removes source from queue of sources,
  *    then read block from source and then put source back to queue of available sources.
  */

namespace DB
{
/** Union mode.
  */
enum class StreamUnionMode
{
    Basic = 0, /// take out blocks
    ExtraInfo /// take out blocks + additional information
};

/// Example of the handler.
struct ParallelInputsHandler
{
    /// Processing the data block.
    void onBlock(Block & /*block*/, size_t /*thread_num*/) {}

    /// Processing the data block + additional information.
    void onBlock(Block & /*block*/, BlockExtraInfo & /*extra_info*/, size_t /*thread_num*/) {}

    /// Called for each thread, when the thread has nothing else to do.
    /// Due to the fact that part of the sources has run out, and now there are fewer sources left than streams.
    /// Called if the `onException` method does not throw an exception; is called before the `onFinish` method.
    void onFinishThread(size_t /*thread_num*/) {}

    /// Blocks are over. Due to the fact that all sources ran out or because of the cancellation of work.
    /// This method is always called exactly once, at the end of the work, if the `onException` method does not throw an exception.
    void onFinish() {}

    /// Exception handling. It is reasonable to call the ParallelInputsProcessor::cancel method in this method, and also pass the exception to the main thread.
    void onException(std::exception_ptr & /*exception*/, size_t /*thread_num*/) {}
};


template <typename Handler, StreamUnionMode mode = StreamUnionMode::Basic>
class ParallelInputsProcessor
{
public:
    /** additional_inputs_at_end - if not empty,
      *  then the blocks from the sources will start to be processed only after all other sources are processed.
      *
      * Intended for implementation of FULL and RIGHT JOIN
      * - where you must first make JOIN in parallel, while noting which keys are not found,
      *   and only after the completion of this work, create blocks of keys that are not found.
      */
    ParallelInputsProcessor(
        const BlockInputStreams & inputs_,
        const BlockInputStreams & additional_inputs_at_end_,
        size_t max_threads_,
        Handler & handler_,
        const LoggerPtr & log_)
        : inputs(inputs_)
        , additional_inputs_at_end(additional_inputs_at_end_)
        , max_threads(std::min(std::max(inputs_.size(), additional_inputs_at_end_.size()), max_threads_))
        , handler(handler_)
        , working_inputs(inputs_)
        , working_additional_inputs(additional_inputs_at_end_)
        , log(log_)
    {}

    ~ParallelInputsProcessor()
    {
        try
        {
            wait();
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }

    /// Start background threads, start work.
    void process()
    {
        if (!thread_manager)
            thread_manager = newThreadManager();
        active_threads = max_threads;
        for (size_t i = 0; i < max_threads; ++i)
            thread_manager->schedule(true, handler.getName(), [this, i] { this->thread(i); });
    }

    /// Ask all sources to stop earlier than they run out.
    void cancel(bool kill)
    {
        working_inputs.available_inputs.cancel();
        working_additional_inputs.available_inputs.cancel();

        cancelStreams(inputs, kill);
        cancelStreams(additional_inputs_at_end, kill);
    }

    /// Wait until all threads are finished, before the destructor.
    void wait()
    {
        if (thread_manager)
        {
            thread_manager->wait();
            thread_manager.reset();
        }
    }

    size_t getNumActiveThreads() const { return active_threads; }

    size_t getMaxThreads() const { return max_threads; }

private:
    /// Single source data
    struct InputData
    {
        BlockInputStreamPtr in;
        size_t i; /// The source number (for debugging).

        InputData()
            : i(0)
        {}
        InputData(const BlockInputStreamPtr & in_, size_t i_)
            : in(in_)
            , i(i_)
        {}
    };

    struct WorkingInputs
    {
        explicit WorkingInputs(const BlockInputStreams & inputs_)
            : available_inputs(inputs_.size())
            , active_inputs(inputs_.size())
            , unprepared_inputs(inputs_.size())
        {
            for (size_t i = 0; i < inputs_.size(); ++i)
                unprepared_inputs.emplace(inputs_[i], i);
        }
        /** A set of available sources that are not currently processed by any thread.
          * Each thread takes one source from this set, takes a block out of the source (at this moment the source does the calculations)
          *  and (if the source is not run out), puts it back into the set of available sources.
          *
          * The question arises what is better to use:
          * - the queue (just processed source will be processed the next time later than the rest)
          * - stack (just processed source will be processed as soon as possible).
          *
          * The stack is better than the queue when you need to do work on reading one source more consequentially,
          *  and theoretically, this allows you to achieve more consequent/consistent reads from the disk.
          *
          * But when using the stack, there is a problem with distributed query processing:
          *  data is read only from a part of the servers, and on the other servers
          * a timeout occurs during send, and the request processing ends with an exception.
          *
          * Therefore, a queue is used. This can be improved in the future.
          */
        using AvailableInputs = MPMCQueue<InputData>;
        AvailableInputs available_inputs;

        /// How many active input streams.
        std::atomic<size_t> active_inputs;

        /** For parallel preparing (readPrefix) child streams.
          * First, streams are located here.
          * After a stream was prepared, it is moved to "available_inputs" for reading.
          */
        using UnpreparedInputs = MPMCQueue<InputData>;
        UnpreparedInputs unprepared_inputs;
    };

    void cancelStreams(const BlockInputStreams & streams, bool kill)
    {
        for (const auto & input : streams)
        {
            if (auto * p_child = dynamic_cast<IProfilingBlockInputStream *>(&*input))
            {
                try
                {
                    p_child->cancel(kill);
                }
                catch (...)
                {
                    /** If you can not ask one or more sources to stop.
                      * (for example, the connection is broken for distributed query processing)
                      * - then do not care.
                      */
                    LOG_ERROR(log, "Exception while cancelling {}", p_child->getName());
                }
            }
        }
    }

    void publishPayload(BlockInputStreamPtr & stream, Block & block, size_t thread_num)
    {
        if constexpr (mode == StreamUnionMode::Basic)
            handler.onBlock(block, thread_num);
        else
        {
            BlockExtraInfo extra_info = stream->getBlockExtraInfo();
            handler.onBlock(block, extra_info, thread_num);
        }
    }

    void thread(size_t thread_num)
    {
        work(thread_num, working_inputs);
        work(thread_num, working_additional_inputs);

        handler.onFinishThread(thread_num);

        if (0 == --active_threads)
        {
            handler.onFinish();
        }
    }

    void work(size_t thread_num, WorkingInputs & work)
    {
        std::exception_ptr exception;

        try
        {
            loop(thread_num, work);
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        if (exception)
        {
            handler.onException(exception, thread_num);
        }
    }

    /// This function may be called in different threads.
    /// If no exception occurs, we can ensure that the work is all done when the function
    /// returns in any thread.
    void loop(size_t thread_num, WorkingInputs & work)
    {
        if (work.active_inputs == 0)
        {
            return;
        }

        InputData input;

        while (work.unprepared_inputs.tryPop(input) == MPMCQueueResult::OK)
        {
            input.in->readPrefix();

            work.available_inputs.push(input);
        }

        // The condition is false when all input streams are exhausted or
        // an exception occurred then the queue was cancelled.
        while (work.available_inputs.pop(input) == MPMCQueueResult::OK)
        {
            /// The main work.
            Block block = input.in->read();

            if (block)
            {
                work.available_inputs.push(input);
                publishPayload(input.in, block, thread_num);
            }
            else
            {
                if (0 == --work.active_inputs)
                {
                    work.available_inputs.finish();
                    break;
                }
            }
        }
    }

    const BlockInputStreams inputs;
    const BlockInputStreams additional_inputs_at_end;
    unsigned max_threads;

    Handler & handler;

    std::shared_ptr<ThreadManager> thread_manager;

    WorkingInputs working_inputs;
    WorkingInputs working_additional_inputs;

    /// How many sources ran out.
    std::atomic<size_t> active_threads{0};

    const LoggerPtr log;
};


} // namespace DB
