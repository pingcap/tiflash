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
class HighValidBigFileGCWorkload
    : public StressWorkload
    , public StressWorkloadFunc<HighValidBigFileGCWorkload>
{
public:
    explicit HighValidBigFileGCWorkload(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "HighValidBigPageFileGCWorkload"; }

    static UInt64 mask() { return 1 << 0; }

    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored"
            "`paths` will only used first one. which is {}. Data will store in {}"
            "Please cleanup folder after this test."
            "The current workload will generate 9G data, and GC will be performed at the end.",
            options.paths[0],
            options.paths[0] + "/" + name());
    }

    void run() override
    {
        metrics_dumper = std::make_shared<PSMetricsDumper>(1, options.logger);
        metrics_dumper->start();

        // For safe , setup timeout.
        stress_time = std::make_shared<StressTimeout>(100, options.logger);
        stress_time->start();

        // Generate 8G data in the same Pagefile
        {
            stop_watch.start();

            DB::PageStorageConfig config;
            config.file_max_size = 8ULL * DB::GB;
            config.file_roll_size = 8ULL * DB::GB;
            initPageStorage(config, name());

            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBufferSizeRange(100ULL * DB::MB, 100ULL * DB::MB);
                writer->setBatchBufferLimit(8ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
            });

            pool.joinAll();
            stop_watch.stop();
            onDumpResult();
        }

        LOG_INFO(options.logger, "Already generator an 8G page file");

        // Generate normal data in the same Pagefile
        {
            DB::PageStorageConfig config;
            config.file_max_size = DB::PAGE_FILE_MAX_SIZE;
            config.file_roll_size = DB::PAGE_FILE_ROLL_SIZE;
            initPageStorage(config, name());
            stop_watch.start();
            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBufferSizeRange(2ULL * DB::MB, 2ULL * DB::MB);
                writer->setBatchBufferLimit(1ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
            });

            pool.joinAll();
            stop_watch.stop();
            onDumpResult();
        }

        gc = std::make_shared<PSGc>(ps, options.gc_interval_s);
        gc->doGcOnce();
        gc_time_ms = gc->getElapsedMilliseconds();
        {
            stop_watch.start();
            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBufferSizeRange(2ULL * DB::MB, 2ULL * DB::MB);
                writer->setBatchBufferLimit(1ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
            });

            pool.joinAll();
            stop_watch.stop();
            onDumpResult();
        }

        gc->doGcOnce();
    }

    bool verify() override { return (gc_time_ms < 1 * 1000); }

    void onFailed() override
    {
        LOG_WARNING(options.logger, "GC time is {} , it should not bigger than {} ", gc_time_ms, 1 * 1000);
    }

private:
    UInt64 gc_time_ms = 0;
};
} // namespace DB::PS::tests
