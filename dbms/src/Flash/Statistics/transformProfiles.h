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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Operators/OperatorProfileInfo.h>

namespace DB
{
template <typename FF>
void transformProfileForStream(DAGContext & dag_context, const String & executor_id, FF && ff)
{
    const auto & profile_streams_map = dag_context.getProfileStreamsMap();
    auto it = profile_streams_map.find(executor_id);
    if (it != profile_streams_map.end())
    {
        for (const auto & input_stream : it->second)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get()); p_stream)
            {
                ff(*p_stream);
            }
        }
    }
}

template <typename FF>
void transformProfileForPipeline(DAGContext & dag_context, const String & executor_id, FF && ff)
{
    const auto & operator_profiles_map = dag_context.getOperatorProfileInfosMap();
    auto it = operator_profiles_map.find(executor_id);
    if (it != operator_profiles_map.end())
    {
        for (const auto & profile_info : it->second)
            ff(*profile_info);
    }
}

template <typename FF>
void transformInBoundIOProfileForStream(DAGContext & dag_context, const String & executor_id, FF && ff)
{
    const auto & io_stream_map = dag_context.getInBoundIOInputStreamsMap();
    auto it = io_stream_map.find(executor_id);
    if (it != io_stream_map.end())
    {
        for (const auto & io_stream : it->second)
            ff(*io_stream);
    }
}

template <typename FF>
void transformInBoundIOProfileForPipeline(DAGContext & dag_context, const String & executor_id, FF && ff)
{
    const auto & operator_profiles_map = dag_context.getInboundIOProfileInfosMap();
    auto it = operator_profiles_map.find(executor_id);
    if (it != operator_profiles_map.end())
    {
        for (const auto & profile_info : it->second)
            ff(*profile_info);
    }
}
} // namespace DB
