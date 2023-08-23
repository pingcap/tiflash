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
class HoldSnapshotsLongTime
    : public StressWorkload
    , public StressWorkloadFunc<HoldSnapshotsLongTime>
{
public:
    explicit HoldSnapshotsLongTime(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "HoldSnapshotsLongTime"; }

    static UInt64 mask() { return 1 << 6; }

private:
    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored"
            "`paths` will only used first one. which is {}. Data will store in {}"
            "Please cleanup folder after this test."
            "The current workload will elapse near 60 seconds and generator 100 snapshot in memory."
            "Then do NORMAL GC + SKIP GC at the last.",
            options.paths[0],
            options.paths[0] + "/" + name());
    }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        DB::PageStorageConfig config;
        initPageStorage(config, name());

        startBackgroundTimer();

        // 90-100 snapshots will be generated.
        {
            stop_watch.start();
            startWriter<PSWindowWriter>(options.num_writers, [](std::shared_ptr<PSWindowWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBufferSizeRange(10 * 1024, 1 * DB::MB);
                writer->setNormalDistributionSigma(13);
            });

            startReader<PSSnapshotReader>(1, [](std::shared_ptr<PSSnapshotReader> reader) -> void {
                reader->setSnapshotGetIntervalMs(600);
            });

            pool.joinAll();
            stop_watch.stop();
        }

        gc = std::make_shared<PSGc>(ps, options.gc_interval_s);
        // Normal GC
        gc->doGcOnce();

        readers.clear();

        // Skip GC
        gc->doGcOnce();
    }

    bool verify() override { return true; }
};
} // namespace DB::PS::tests
