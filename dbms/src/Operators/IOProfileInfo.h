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

#include <Flash/Coprocessor/RemoteExecutionSummary.h>
#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Operators/OperatorProfileInfo.h>

#include "Common/Exception.h"

namespace DB
{
struct IOProfileInfo;
using IOProfileInfoPtr = std::shared_ptr<IOProfileInfo>;
using IOProfileInfos = std::vector<IOProfileInfoPtr>;

/// Record the statistics of the IO operator.
struct IOProfileInfo
{
    IOProfileInfo(const OperatorProfileInfoPtr & operator_info_, bool is_local_)
        : operator_info(operator_info_)
        , is_local(is_local_)
    {}

    static IOProfileInfoPtr createForLocal(const OperatorProfileInfoPtr & profile_info)
    {
        return std::make_shared<IOProfileInfo>(profile_info, true);
    }

    static IOProfileInfoPtr createForRemote(const OperatorProfileInfoPtr & profile_info, size_t connections)
    {
        auto info = std::make_shared<IOProfileInfo>(profile_info, false);
        info->connection_profile_infos.resize(connections);
        return info;
    }

    static IOProfileInfoPtr createForRemote(
        const OperatorProfileInfoPtr & profile_info,
        size_t connections,
        const ConnectionProfileInfo::ConnTypeVec & conn_type_vec)
    {
        RUNTIME_CHECK(connections == conn_type_vec.size());
        auto info = std::make_shared<IOProfileInfo>(profile_info, false);
        info->connection_profile_infos.resize(connections);
        for (size_t i = 0; i < connections; ++i)
        {
            info->connection_profile_infos[i].type = conn_type_vec[i];
        }
        return info;
    }

    OperatorProfileInfoPtr operator_info;

    const bool is_local;
    std::vector<ConnectionProfileInfo> connection_profile_infos{};
    RemoteExecutionSummary remote_execution_summary{};
};
} // namespace DB
