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

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/TiFlashMetrics.h>

namespace DB
{
TiFlashMetrics & TiFlashMetrics::instance()
{
    static TiFlashMetrics inst; // Instantiated on first use.
    return inst;
}

TiFlashMetrics::TiFlashMetrics()
{
    registered_profile_events.reserve(ProfileEvents::end());
    for (ProfileEvents::Event event = 0; event < ProfileEvents::end(); event++)
    {
        std::string name{ProfileEvents::getDescription(event)};
        auto & family
            = prometheus::BuildGauge().Name(profile_events_prefix + name).Help("System profile event " + name).Register(*registry);
        registered_profile_events.push_back(&family.Add({}));
    }

    registered_current_metrics.reserve(CurrentMetrics::end());
    for (CurrentMetrics::Metric metric = 0; metric < CurrentMetrics::end(); metric++)
    {
        std::string name{CurrentMetrics::getDescription(metric)};
        auto & family
            = prometheus::BuildGauge().Name(current_metrics_prefix + name).Help("System current metric " + name).Register(*registry);
        registered_current_metrics.push_back(&family.Add({}));
    }

    auto prometheus_name = TiFlashMetrics::current_metrics_prefix + std::string("StoreSizeUsed");
    registered_keypace_store_used_family = &prometheus::BuildGauge().Name(prometheus_name).Help("Store size used of keyspace").Register(*registry);
    store_used_total_metric = &registered_keypace_store_used_family->Add({{"keyspace_id", ""}, {"type", "all_used"}});
}

} // namespace DB
