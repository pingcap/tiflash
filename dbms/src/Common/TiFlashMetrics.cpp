#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/TiFlashMetrics.h>

namespace DB
{

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
}

} // namespace DB