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

#include <Flash/Statistics/BaseRuntimeStatistics.h>

namespace DB
{
class ExecutorStatisticsBase
{
public:
    virtual String toJson() const = 0;

    virtual void setChild(const String & child_id) = 0;

    virtual void setChildren(const std::vector<String> & children) = 0;

    virtual void collectRuntimeDetail() = 0;

    bool isSourceExecutor() const { return is_source_executor; }

    virtual ~ExecutorStatisticsBase() = default;

    const BaseRuntimeStatistics & getBaseRuntimeStatistics() const { return base; }

    UInt64 processTimeForJoinBuild() const { return process_time_for_join_build; }

protected:
    BaseRuntimeStatistics base;
    UInt64 process_time_for_join_build = 0;
    bool is_source_executor = false;
};

using ExecutorStatisticsPtr = std::shared_ptr<ExecutorStatisticsBase>;
} // namespace DB
