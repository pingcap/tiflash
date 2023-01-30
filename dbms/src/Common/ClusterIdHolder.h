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

#include <Common/nocopyable.h>

#include <string>

namespace DB
{
class ClusterIdHolder
{
public:
    ClusterIdHolder() = default;

    DISALLOW_COPY_AND_MOVE(ClusterIdHolder);

    static ClusterIdHolder & instance()
    {
        static ClusterIdHolder inst; // Instantiated on first use.
        return inst;
    }

    void setClusterId(const std::string & set_cluster_id)
    {
        cluster_id = set_cluster_id;
    }

    const std::string & getClusterId() const
    {
        return cluster_id;
    }

private:
    std::string cluster_id = "default";
};
}
