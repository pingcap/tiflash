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

#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct AggImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Agg";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_aggregation();
    }
};
using AggStatistics = ExecutorStatistics<AggImpl>;

struct WindowImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Window";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_window();
    }
};
using WindowStatistics = ExecutorStatistics<WindowImpl>;

struct SortImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Sort";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_sort();
    }
};
using SortStatistics = ExecutorStatistics<SortImpl>;

struct ExpandImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Expand";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_expand();
    }
};
using ExpandStatistics = ExecutorStatistics<ExpandImpl>;

struct FilterImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Selection";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_selection();
    }
};
using FilterStatistics = ExecutorStatistics<FilterImpl>;

struct LimitImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Limit";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_limit();
    }
};
using LimitStatistics = ExecutorStatistics<LimitImpl>;

struct ProjectImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Projection";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_projection();
    }
};
using ProjectStatistics = ExecutorStatistics<ProjectImpl>;

struct TopNImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "TopN";

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_topn();
    }
};
using TopNStatistics = ExecutorStatistics<TopNImpl>;
} // namespace DB
