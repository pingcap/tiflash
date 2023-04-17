// Copyright 2023 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Executor/PipelineExecutorStatus.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <assert.h>

#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char random_pipeline_model_event_schedule_failpoint[];
extern const char random_pipeline_model_event_finish_failpoint[];
} // namespace FailPoints

// if any exception throw here, we should record err msg and then cancel the query.
#define CATCH                                                    \
    catch (...)                                                  \
    {                                                            \
        LOG_WARNING(log, "error occurred and cancel the query"); \
        exec_status.onErrorOccurred(std::current_exception());   \
    }

void Event::addInput(const EventPtr & input)
{
    assert(status == EventStatus::INIT);
    assert(input.get() != this);
    input->addOutput(shared_from_this());
    ++unfinished_inputs;
    is_source = false;
}

void Event::addOutput(const EventPtr & output)
{
    /// Output will also be added in the Finished state, as can be seen in the `insertEvent`.
    assert(status == EventStatus::INIT || status == EventStatus::FINISHED);
    assert(output.get() != this);
    outputs.push_back(output);
}

void Event::insertEvent(const EventPtr & insert_event) noexcept
{
    assert(insert_event);
    assert(status == EventStatus::FINISHED);
    /// eventA───────►eventB ===> eventA─────────────►eventB
    ///                             │                    ▲
    ///                             └────►insert_event───┘
    for (const auto & output : outputs)
    {
        assert(output);
        output->addInput(insert_event);
    }
    insert_event->addInput(shared_from_this());
    RUNTIME_ASSERT(!insert_event->prepareForSource());
}

void Event::onInputFinish() noexcept
{
    auto cur_value = unfinished_inputs.fetch_sub(1);
    assert(cur_value >= 1);
    if (1 == cur_value)
        schedule();
}

bool Event::prepareForSource()
{
    assert(status == EventStatus::INIT);
    if (is_source)
    {
        // For source event, `exec_status.onEventSchedule()` needs to be called before schedule.
        // Suppose there are two source events, A and B, a possible sequence of calls is:
        // `A.prepareForSource --> B.prepareForSource --> A.schedule --> A.finish --> B.schedule --> B.finish`.
        // if `exec_status.onEventSchedule()` be called in schedule just like non-source event,
        // `exec_status.wait` and `result_queue.pop` may return early.
        switchStatus(EventStatus::INIT, EventStatus::SCHEDULED);
        exec_status.onEventSchedule();
        return true;
    }
    else
    {
        return false;
    }
}

void Event::schedule() noexcept
{
    assert(0 == unfinished_inputs);
    if (is_source)
    {
        RUNTIME_ASSERT(status == EventStatus::SCHEDULED);
    }
    else
    {
        // for is_source == true, `exec_status.onEventSchedule()` has been called in `prepareForSource`.
        switchStatus(EventStatus::INIT, EventStatus::SCHEDULED);
        exec_status.onEventSchedule();
    }
    MemoryTrackerSetter setter{true, mem_tracker.get()};
    std::vector<TaskPtr> tasks;
    try
    {
        tasks = scheduleImpl();
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_event_schedule_failpoint);
    }
    CATCH
    if (!tasks.empty())
    {
        scheduleTasks(tasks);
    }
    else
    {
        // if no task is scheduled here, we should call finish directly.
        finish();
    }
}

void Event::finish() noexcept
{
    switchStatus(EventStatus::SCHEDULED, EventStatus::FINISHED);
    MemoryTrackerSetter setter{true, mem_tracker.get()};
    try
    {
        finishImpl();
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_event_finish_failpoint);
    }
    CATCH
    // If query has already been cancelled, it will not trigger outputs.
    if (likely(!exec_status.isCancelled()))
    {
        // finished processing the event, now we can schedule output events.
        for (auto & output : outputs)
        {
            assert(output);
            output->onInputFinish();
            output.reset();
        }
    }
    // Release all output, so that the event that did not call `finishImpl`
    // because of `exec_status.isCancelled()` will be destructured before the end of `exec_status.wait`.
    outputs.clear();
    // In order to ensure that `exec_status.wait()` doesn't finish when there is an active event,
    // we have to call `exec_status.onEventFinish()` here,
    // since `exec_status.onEventSchedule()` will have been called by outputs.
    // The call order will be `eventA++ ───► eventB++ ───► eventA-- ───► eventB-- ───► exec_status.await finished`.
    exec_status.onEventFinish();
}

void Event::scheduleTasks(std::vector<TaskPtr> & tasks) noexcept
{
    assert(!tasks.empty());
    assert(0 == unfinished_tasks);
    unfinished_tasks = tasks.size();
    assert(status != EventStatus::FINISHED);
    // If query has already been cancelled, we can skip scheduling tasks.
    // And then tasks will be destroyed and call `onTaskFinish`.
    if (likely(!exec_status.isCancelled()))
    {
        LOG_DEBUG(log, "{} tasks scheduled by event", tasks.size());
        TaskScheduler::instance->submit(tasks);
    }
}

void Event::onTaskFinish() noexcept
{
    assert(status != EventStatus::FINISHED);
    int32_t remaining_tasks = unfinished_tasks.fetch_sub(1) - 1;
    assert(remaining_tasks >= 0);
    LOG_DEBUG(log, "one task finished, {} tasks remaining", remaining_tasks);
    if (0 == remaining_tasks)
        finish();
}

void Event::switchStatus(EventStatus from, EventStatus to) noexcept
{
    RUNTIME_ASSERT(status.compare_exchange_strong(from, to));
    LOG_DEBUG(log, "switch status: {} --> {}", magic_enum::enum_name(from), magic_enum::enum_name(to));
}

#undef CATCH

} // namespace DB
