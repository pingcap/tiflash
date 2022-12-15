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

#include <Common/Exception.h>
#include <Flash/Pipeline/Event.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <assert.h>

#include <magic_enum.hpp>

namespace DB
{
void Event::addDependency(const EventPtr & dependency)
{
    assert(status != EventStatus::FINISHED);
    dependency->addNext(shared_from_this());
    ++unfinished_dependencies;
}

bool Event::isNonDependent()
{
    assert(status == EventStatus::INIT);
    return 0 == unfinished_dependencies;
}

void Event::addNext(const EventPtr & next)
{
    assert(status != EventStatus::FINISHED);
    next_events.push_back(next);
}

void Event::insertEvent(const EventPtr & replacement)
{
    assert(replacement && replacement->status == EventStatus::INIT);
    assert(status != EventStatus::FINISHED);
    std::swap(replacement->next_events, next_events);
    replacement->addDependency(shared_from_this());
}

void Event::completeDependency()
{
    auto cur_value = unfinished_dependencies.fetch_sub(1);
    if (cur_value <= 1)
        schedule();
}

void Event::schedule()
{
    setMemoryTracker();
    switchStatus(EventStatus::INIT, EventStatus::SCHEDULED);
    if (scheduleImpl())
        finish();
}

void Event::finish()
{
    setMemoryTracker();
    switchStatus(EventStatus::SCHEDULED, EventStatus::FINISHED);
    if (finishImpl())
    {
        // finished processing the event, now we can schedule events that depend on this event
        for (auto & next_event : next_events)
            next_event->completeDependency();
    }
    finalizeFinish();
}

void Event::scheduleTask(std::vector<TaskPtr> & tasks)
{
    assert(!tasks.empty());
    unfinished_tasks = tasks.size();
    assert(status != EventStatus::FINISHED);
    TaskScheduler::instance->submit(tasks);
}

void Event::finishTask()
{
    assert(status != EventStatus::FINISHED);
    auto cur_value = unfinished_tasks.fetch_sub(1);
    if (cur_value <= 1)
        finish();
}

void Event::toError(std::string && err_msg)
{
    exec_status.toError(std::move(err_msg));
}

void Event::switchStatus(EventStatus from, EventStatus to)
{
    RUNTIME_CHECK(status.compare_exchange_strong(from, to));
}
} // namespace DB
