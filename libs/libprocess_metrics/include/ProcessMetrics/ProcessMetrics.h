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

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ProcessMetricsInfo
{
    uint64_t cpu_total;
    uint64_t vsize;
    uint64_t rss;
    uint64_t rss_anon;
    uint64_t rss_file;
    uint64_t rss_shared;
    int64_t start_time;
};

ProcessMetricsInfo get_process_metrics();

#ifdef __cplusplus
}
#endif
