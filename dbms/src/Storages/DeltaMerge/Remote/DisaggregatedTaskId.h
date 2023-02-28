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

#include <Flash/Mpp/MPPTaskId.h>
#include <fmt/format.h>
#include <kvproto/disaggregated.pb.h>

namespace DB::DM
{
class DisaggregatedTaskId
{
public:
    static const DisaggregatedTaskId unknown_disaggregated_task_id;

public:
    DisaggregatedTaskId()
        : DisaggregatedTaskId(MPPTaskId::unknown_mpp_task_id, "")
    {}

    DisaggregatedTaskId(MPPTaskId task_id, String executor_id_)
        : mpp_task_id(std::move(task_id))
        , executor_id(std::move(executor_id_))
    {
    }

    explicit DisaggregatedTaskId(const disaggregated::DisaggregatedTaskMeta & task_meta);

    disaggregated::DisaggregatedTaskMeta toMeta() const;

    const MPPTaskId mpp_task_id;
    const String executor_id;
};

bool operator==(const DisaggregatedTaskId & lhs, const DisaggregatedTaskId & rhs);
} // namespace DB::DM

template <>
struct fmt::formatter<DB::DM::DisaggregatedTaskId>
{
    static constexpr auto parse(format_parse_context & ctx) -> decltype(ctx.begin())
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::DM::DisaggregatedTaskId & task_id, FormatContext & ctx) const -> decltype(ctx.out())
    {
        if (task_id.mpp_task_id.isUnknown())
            return format_to(ctx.out(), "DisTaskId<N/A>");
        return format_to(
            ctx.out(),
            "DisTaskId<{},executor={}>",
            task_id.mpp_task_id.toString(),
            task_id.executor_id);
    }
};

namespace std
{
template <>
class hash<DB::DM::DisaggregatedTaskId>
{
public:
    size_t operator()(const DB::DM::DisaggregatedTaskId & id) const
    {
        return hash<DB::MPPTaskId>()(id.mpp_task_id) ^ hash<String>()(id.executor_id);
    }
};
} // namespace std
