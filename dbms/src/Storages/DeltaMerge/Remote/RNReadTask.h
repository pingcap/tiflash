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

#include <Storages/DeltaMerge/SegmentReadTask.h>


#include <boost/noncopyable.hpp>
#include <memory>

namespace DB::DM::Remote
{

class RNReadTask;
using RNReadTaskPtr = std::shared_ptr<RNReadTask>;

/// "Remote read" is simply reading from these remote segments.
class RNReadTask : boost::noncopyable
{
public:
    const std::vector<SegmentReadTaskPtr> segment_read_tasks;

    static RNReadTaskPtr create(const std::vector<SegmentReadTaskPtr> & segment_read_tasks_)
    {
        return std::shared_ptr<RNReadTask>(new RNReadTask(segment_read_tasks_));
    }

private:
    explicit RNReadTask(const std::vector<SegmentReadTaskPtr> & segment_read_tasks_)
        : segment_read_tasks(segment_read_tasks_)
    {}
};
} // namespace DB::DM::Remote
