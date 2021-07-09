#include <Interpreters/Settings.h>
#include <Storages/Page/ConfigSettings.h>

namespace DB
{

void mergeConfigFromSettings(const DB::Settings & settings, PageStorage::Config & config)
{
    config.open_file_max_idle_time = Seconds(settings.dt_open_file_max_idle_seconds);

    {
        // The probability is [0~1000] out of 1000
        Int64 prob                          = settings.dt_page_gc_low_write_prob * 1000;
        prob                                = std::max(0, std::min(1000, prob));
        config.prob_do_gc_when_write_is_low = prob;
    }

    // Load config for expected number of max legacy files
    if (settings.dt_page_num_max_expect_legacy_files != 0)
        config.gc_max_expect_legacy_files = settings.dt_page_num_max_expect_legacy_files;
    if (settings.dt_page_num_max_gc_valid_rate > 0.0)
        config.gc_max_valid_rate_bound = settings.dt_page_num_max_gc_valid_rate;
}

PageStorage::Config getConfigFromSettings(const DB::Settings & settings)
{
    PageStorage::Config config;
    mergeConfigFromSettings(settings, config);
    return config;
}

} // namespace DB
