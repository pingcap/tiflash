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
class InstanceLabelHolder : public ext::Singleton<InstanceLabelHolder>
{
public:
    void init(const Poco::Util::LayeredConfiguration & conf);

    std::pair<std::string, std::string> getClusterIdLabel();
    std::pair<std::string, std::string> getInstanceIdLabel();

private:
    std::mutex mu;
    bool label_got = false;
    std::string cluster_id{"unknown"};
    std::string instance_id{"unknown"};

    LoggerPtr log = Logger::get();
};
} // namespace DB
