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

#include <Common/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <ext/singleton.h>
#include <mutex>
#include <string>

namespace DB
{
// Holds the labels for tiflash compute node metrics.
class ComputeLabelHolder : public ext::Singleton<ComputeLabelHolder>
{
public:
    void init(const Poco::Util::LayeredConfiguration & conf);

    std::pair<std::string, std::string> getClusterIdLabel();
    std::pair<std::string, std::string> getProcessIdLabel();

private:
    std::mutex mu;
    bool label_got = false;
    // the id of tiflash compute cluster
    std::string cluster_id{"unknown"};
    // the id of tiflash compute process, used to distinguish between processes that have been started multiple times.
    // Format: `compute_${service_addr}_${start_time_second_utc}`
    std::string process_id{"unknown"};

    LoggerPtr log = Logger::get();
};
} // namespace DB
