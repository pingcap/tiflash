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

#include <Storages/Page/workload/PSWorkload.h>

namespace DB::PS::tests
{
class HeavyMemoryCostInGC
    : public StressWorkload
    , public StressWorkloadFunc<HeavyMemoryCostInGC>
{
public:
    explicit HeavyMemoryCostInGC(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "HeavyMemoryCostInGCWorkload"; }

    static UInt64 mask() { return 1 << 1; }

    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored"
            "`paths` will only used first one. which is {}. Data will store in {}"
            "Please cleanup folder after this test."
            "The current workload will elapse near 30 seconds, and GC will be performed at the end.",
            options.paths[0],
            options.paths[0] + "/" + name());
    }

    void run() override
    {
        stop_watch.start();

        DB::PageStorageConfig config;
        initPageStorage(config, name());

        startBackgroundTimer();

        startWriter<PSCommonWriter>(options.num_writers, [](std::shared_ptr<PSCommonWriter> writer) -> void {
            writer->setBatchBufferNums(100);
            writer->setBufferSizeRange(1, 1);
        });

        pool.joinAll();
        stop_watch.stop();

        gc = std::make_shared<PSGc>(ps, options.gc_interval_s);
        gc->doGcOnce();
    }

    bool verify() override { return (metrics_dumper->getMemoryPeak() < 5UL * 1024 * 1024); }

    void onFailed() override
    {
        LOG_WARNING(
            options.logger,
            "Memory Peak is {}, it should not bigger than {}",
            metrics_dumper->getMemoryPeak(),
            5 * 1024 * 1024);
    }
};
} // namespace DB::PS::tests
