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

#include <Common/MPMCQueue.h>
#include <Flash/Pipeline/task/Event.h>

#include <memory>

namespace DB
{
class EventLoop
{
public:
    EventLoop(size_t loop_id_)
        : loop_id(loop_id_)
    {}

    void loop();

    void finish();

    void submit(TaskEvent && event);

private:
    void handleSubmit(TaskEvent & event);

private:
    size_t loop_id;
    MPMCQueue<TaskEvent> event_queue{99999};
};

using EventLoopPtr = std::shared_ptr<EventLoop>;
} // namespace DB
