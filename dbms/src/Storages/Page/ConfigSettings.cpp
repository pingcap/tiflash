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

#include <Interpreters/Settings.h>
#include <Storages/Page/ConfigSettings.h>

namespace DB
{
void mergeConfigFromSettings(const DB::Settings & settings, PageStorageConfig & config)
{
    config.open_file_max_idle_time = settings.dt_open_file_max_idle_seconds;

    {
        // The probability is [0~1000] out of 1000
        Int64 prob = settings.dt_page_gc_low_write_prob * 1000;
        prob = std::max(0, std::min(1000, prob));
        config.prob_do_gc_when_write_is_low = prob;
    }

    // Load config for expected number of max legacy files
    if (settings.dt_page_num_max_expect_legacy_files != 0)
        config.gc_max_expect_legacy_files = settings.dt_page_num_max_expect_legacy_files;
    if (settings.dt_page_num_max_gc_valid_rate > 0.0)
        config.gc_max_valid_rate_bound = settings.dt_page_num_max_gc_valid_rate;

    // V3 setting which export to global setting
    config.blob_heavy_gc_valid_rate = settings.dt_page_gc_threshold;
}

PageStorageConfig getConfigFromSettings(const DB::Settings & settings)
{
    PageStorageConfig config;
    mergeConfigFromSettings(settings, config);
    return config;
}

} // namespace DB
