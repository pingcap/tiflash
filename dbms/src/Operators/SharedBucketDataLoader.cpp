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

#include <Operators/SharedBucketDataLoader.h>
#include <Flash/Pipeline/Schedule/Tasks/EventTask.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Executor/PipelineExecutorStatus.h>

namespace DB
{
namespace
{
class LoadTask : public EventTask
{
public:
    LoadTask(
        MemoryTrackerPtr mem_tracker_,
        const String & req_id,
        PipelineExecutorStatus & exec_status_,
        const EventPtr & event_,
        BucketInput & input_)
        : EventTask(std::move(mem_tracker_), req_id, exec_status_, event_)
        , input(input_)
    {
    }

private:
    ExecTaskStatus doExecuteImpl() override
    {
        return ExecTaskStatus::IO;
    }

    ExecTaskStatus doExecuteIOImpl() override
    {
        input.load();
        return ExecTaskStatus::FINISHED;
    }

    ExecTaskStatus doAwaitImpl() override
    {
        return ExecTaskStatus::IO;
    }

private:
    BucketInput & input;
};

class LoadEvent : public Event
{
public:
    LoadEvent(
        PipelineExecutorStatus & exec_status_,
        MemoryTrackerPtr mem_tracker_,
        const String & req_id,
        SharedBucketDataLoaderPtr loader_)
        : Event(exec_status_, std::move(mem_tracker_), req_id)
        , loader(std::move(loader_))
    {
        assert(loader);
    }

protected:
    std::vector<TaskPtr> scheduleImpl() override
    {
        assert(loader);
        std::vector<TaskPtr> tasks;
        auto load_inputs = loader->getLoadInputs();
        tasks.reserve(load_inputs.size());
        for (const auto & input : load_inputs)
            tasks.push_back(std::make_unique<LoadTask>(mem_tracker, log->identifier(), exec_status, shared_from_this(), *input));
        return tasks;
    }

    void finishImpl() override
    {
        if likely (!exec_status.isCancelled())
        {
            assert(loader);
            loader->storeFromInputToBucketData();
        }
        loader.reset();
    }

private:
    SharedBucketDataLoaderPtr loader;
};
}

SharedBucketDataLoader::SharedBucketDataLoader(
    PipelineExecutorStatus & exec_status_,
    const BlockInputStreams & bucket_streams,
    const String & req_id,
    size_t max_queue_size_)
    : exec_status(exec_status_)
    , log(Logger::get(req_id))
    , max_queue_size(std::max(1, max_queue_size_))
{
    for (const auto & bucket_stream : bucket_streams)
        bucket_inputs.emplace_back(bucket_stream);
    if (bucket_inputs.empty())
        finish();

    exec_status.onEventSchedule();
}

SharedBucketDataLoader::~SharedBucketDataLoader()
{
    exec_status.onEventFinish();   
}

void SharedBucketDataLoader::finish()
{
    assert(!finished);
    finished = true;
    bucket_inputs.clear();
}

std::vector<BucketInput *> SharedBucketDataLoader::getLoadInputs()
{
    assert(!bucket_inputs.empty());
    std::vector<BucketInput *> load_inputs;
    auto mem_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
    auto self_ptr = shared_from_this();
    for (auto & bucket_input : bucket_inputs)
    {
        if (bucket_input.needLoad())
            load_inputs.push_back(&bucket_input);
    }
    return load_inputs;
}

void SharedBucketDataLoader::storeFromInputToBucketData()
{
    assert(!bucket_inputs.empty());

    // get min bucket num.
    Int32 min_bucket_num = NUM_BUCKETS;
    for (auto & bucket_input : bucket_inputs)
        min_bucket_num = std::min(bucket_input.bucketNum(), min_bucket_num);
    if unlikely (min_bucket_num >= NUM_BUCKETS)
    {
        finish();
        return;
    }

    BlocksList bucket_data;
    // store bucket data of min bucket num.
    for (auto & bucket_input : bucket_inputs)
    {
        if (min_bucket_num == bucket_input.bucketNum())
            bucket_data.push_back(bucket_input.moveOutput());
    }
    bucket_data_queue.push(std::move(bucket_data));

    if (bucket_data_queue.size() < max_queue_size)
        submitLoadEvent();
}

void SharedBucketDataLoader::submitLoadEvent()
{
    if unlikely (finished)
        return;

   for (auto it = bucket_inputs.begin(); it != bucket_inputs.end();)
        it = it->needRemoved() ? bucket_inputs.erase(it) : std::next(it);
    if (!bucket_inputs.empty())
    {
        auto mem_tracker = current_memory_tracker ? current_memory_tracker->shared_from_this() : nullptr;
        auto event = std::make_shared<LoadEvent>(exec_status, mem_tracker, log->identifier(), shared_from_this());
        RUNTIME_CHECK(event->prepareForSource());
        event->schedule();
    }
}

bool SharedBucketDataLoader::tryPop(BlocksList & bucket_data)
{
    if unlikely (finished)
        return true;

    if (bucket_data_queue.empty())
        return false;
    bucket_data = std::move(bucket_data_queue.front());
    bucket_data_queue.pop();
    submitLoadEvent();
    return true;
}
} // namespace DB
