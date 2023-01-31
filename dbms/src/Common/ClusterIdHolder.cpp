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

#include <Common/ClusterIdHolder.h>
#include <Common/Exception.h>

namespace DB
{
void ClusterIdHolder::set(const std::string & cluster_id_)
{
    std::lock_guard lock(mu);
    cluster_id = cluster_id_;
}

std::string ClusterIdHolder::get() const
{
    std::lock_guard lock(mu);
    return cluster_id;
}
} // namespace DB
