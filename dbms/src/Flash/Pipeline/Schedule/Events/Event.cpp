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
    assertStatus(EventStatus::INIT);
    RUNTIME_ASSERT(input.get() != this, log, "Cannot create circular dependency");
    input->addOutput(shared_from_this());
    ++unfinished_inputs;
    is_source = false;
}

void Event::addOutput(const EventPtr & output)
{
    assertStatus(EventStatus::INIT);
    RUNTIME_ASSERT(output.get() != this, log, "Cannot create circular dependency");
    outputs.push_back(output);
}

void Event::insertEvent(const EventPtr & insert_event)
{
    assertStatus(EventStatus::FINISHED);
    RUNTIME_ASSERT(insert_event, log, "The insert event cannot be nullptr");
    /// eventA───────►eventB ===> eventA───►insert_event───►eventB
    assert(insert_event->outputs.empty());
    insert_event->outputs = std::move(outputs);
    outputs = {insert_event};
    assert(insert_event->unfinished_inputs == 0);
    insert_event->unfinished_inputs = 1;
    insert_event->is_source = false;
    RUNTIME_ASSERT(!insert_event->prepare(), log, "The insert event cannot be source event");
}

void Event::onInputFinish()
{
    auto cur_value = unfinished_inputs.fetch_sub(1) - 1;
    RUNTIME_ASSERT(
        cur_value >= 0,
        log,
        "unfinished_inputs cannot < 0, but actual value is {}",
        cur_value);
    if (0 == cur_value)
        schedule();
}

bool Event::prepare()
{
    assertStatus(EventStatus::INIT);
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

void Event::addTask(TaskPtr && task)
{
    assertStatus(EventStatus::SCHEDULED);
    ++unfinished_tasks;
    tasks.push_back(std::move(task));
}

void Event::schedule()
{
    RUNTIME_ASSERT(
        0 == unfinished_inputs,
        log,
        "unfinished_inputs must be 0 in `schedule`, but actual value is {}",
        unfinished_inputs);
    if (is_source)
    {
        assertStatus(EventStatus::SCHEDULED);
    }
    else
    {
        // for is_source == true, `exec_status.onEventSchedule()` has been called in `prepare`.
        switchStatus(EventStatus::INIT, EventStatus::SCHEDULED);
        exec_status.onEventSchedule();
    }
    MemoryTrackerSetter setter{true, mem_tracker.get()};
    try
    {
        scheduleImpl();
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_pipeline_model_event_schedule_failpoint);
    }
    CATCH
    scheduleTasks();
}

void Event::scheduleTasks()
{
    assertStatus(EventStatus::SCHEDULED);
    RUNTIME_ASSERT(
        tasks.size() == static_cast<size_t>(unfinished_tasks),
        log,
        "{} does not equal to {}",
        tasks.size(),
        unfinished_tasks);
    if (!tasks.empty())
    {
        TaskScheduler::instance->submit(tasks);
        tasks.clear();
    }
    else
    {
        // if no task is scheduled here, we should call finish directly.
        finish();
    }
}

void Event::onTaskFinish()
{
    assertStatus(EventStatus::SCHEDULED);
    int32_t remaining_tasks = unfinished_tasks.fetch_sub(1) - 1;
    RUNTIME_ASSERT(
        remaining_tasks >= 0,
        log,
        "remaining_tasks must >= 0, but actual value is {}",
        remaining_tasks);
    LOG_DEBUG(log, "one task finished, {} tasks remaining", remaining_tasks);
    if (0 == remaining_tasks)
        finish();
}

void Event::finish()
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
            RUNTIME_ASSERT(output, log, "output event cannot be nullptr");
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

void Event::switchStatus(EventStatus from, EventStatus to)
{
    RUNTIME_ASSERT(
        status.compare_exchange_strong(from, to),
        log,
        "switch from {} to {} fail, because the current status is {}",
        magic_enum::enum_name(from),
        magic_enum::enum_name(to),
        magic_enum::enum_name(status.load()));
    LOG_DEBUG(log, "switch status: {} --> {}", magic_enum::enum_name(from), magic_enum::enum_name(to));
}

void Event::assertStatus(EventStatus expect)
{
    auto cur_status = status.load();
    RUNTIME_ASSERT(
        cur_status == expect,
        log,
        "actual status is {}, but expect status is {}",
        magic_enum::enum_name(cur_status),
        magic_enum::enum_name(expect));
}

#undef CATCH

} // namespace DB
