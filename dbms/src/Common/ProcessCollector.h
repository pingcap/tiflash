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

#include <Common/ProcessCollector_fwd.h>
#include <ProcessMetrics/ProcessMetrics.h>
#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/metric_family.h>
#include <prometheus/registry.h>

namespace DB
{

// Why not use async_metrics for cpu/mem metric:
// 1. ProcessCollector will collect cpu/mem metric when ProcessCollector::Collect() is called, so it's synchronous.
//    Just like the original tiflash-proxy logic.
// 2. Current implentation of async_metrics interval is 15s, it's too large. And this interval also affect pushgateway interval.
//    So better not to mix cpu/mem metrics with async_metrics.
class ProcessCollector : public prometheus::Collectable
{
public:
    std::vector<prometheus::MetricFamily> Collect() const override;

public:
    mutable std::atomic<bool> include_proxy_metrics = {true};
};

} // namespace DB
