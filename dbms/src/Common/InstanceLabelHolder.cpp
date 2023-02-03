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

#include <Common/Exception.h>
#include <Common/InstanceLabelHolder.h>
#include <Common/Logger.h>
#include <common/logger_useful.h>

namespace DB
{
namespace
{
constexpr auto cluster_id_key = "cluster.cluster_id";

auto microsecondsUTC()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
} // namespace

void InstanceLabelHolder::init(const Poco::Util::LayeredConfiguration & conf)
{
    std::lock_guard lock(mu);
    RUNTIME_ASSERT(!label_got, "Can't init after label_got got");
    cluster_id = conf.getString(cluster_id_key, "unknown");
    auto service_addr = conf.getString("flash.service_addr", "unknown");
    std::replace(service_addr.begin(), service_addr.end(), ':', '_');
    std::replace(service_addr.begin(), service_addr.end(), '.', '_');
    instance_id = fmt::format("tiflash_{}_{}", service_addr, microsecondsUTC());
}

std::pair<std::string, std::string> InstanceLabelHolder::getClusterIdLabel()
{
    std::lock_guard lock(mu);
    label_got = true;
    LOG_INFO(Logger::get(), "get cluster id: {}", cluster_id);
    return {"cluster_id", cluster_id};
}

std::pair<std::string, std::string> InstanceLabelHolder::getInstanceIdLabel()
{
    std::lock_guard lock(mu);
    label_got = true;
    LOG_INFO(Logger::get(), "get instance id: {}", instance_id);
    return {"instance_id", instance_id};
}

} // namespace DB
