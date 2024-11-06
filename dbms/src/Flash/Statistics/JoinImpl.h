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

#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct JoinImpl
{
    static constexpr bool has_extra_info = true;

    static constexpr auto type = "Join";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_join(); }

    static bool isSourceExecutor() { return false; }
};

using JoinStatisticsBase = ExecutorStatistics<JoinImpl>;

class JoinStatistics : public JoinStatisticsBase
{
public:
    JoinStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    size_t peak_build_bytes_usage = 0;
    String build_side_child;
    bool is_spill_enabled = false;
    bool is_spilled = false;

    BaseRuntimeStatistics join_build_base;

protected:
    void appendExtraJson(FmtBuffer &) const override;
    void collectExtraRuntimeDetail() override;
};
} // namespace DB
