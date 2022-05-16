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

#include <PSWorkload.h>

class HeavyRestore : public StressWorkload
    , public StressWorkloadFunc<HeavyRestore>
{
public:
    explicit HeavyRestore(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name()
    {
        return "HeavyRestore";
    }

    static UInt64 mask()
    {
        return 1 << 8;
    }

    void onDumpResult() override
    {
        StressWorkload::onDumpResult();

        LOG_INFO(options.logger, fmt::format("Restore in {}ms.", restore_stop_watch.elapsedMilliseconds()));
    }

private:
    String desc() override
    {
        return fmt::format("Some of options will be ignored"
                           "`paths` will only used first one. which is {}. Data will store in {}"
                           "Please cleanup folder after this test.",
                           options.paths[0],
                           options.paths[0] + "/" + name());
    }

    void run() override
    {
        size_t num_writers = options.num_writers;
        UInt64 fix_size = 100 * DB::GB;
        UInt64 single_page_size = 512 * 1024;
        UInt64 batch_size = 10;
        UInt64 page_numbers = fix_size / single_page_size / batch_size / num_writers;

        DB::PageStorage::Config config;
        initPageStorage(config, name());

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        {
            stop_watch.start();
            startWriter<PSIncreaseWriter>(options.num_writers, [&](std::shared_ptr<PSIncreaseWriter> writer) -> void {
                writer->setBatchBufferNums(batch_size);
                writer->setBatchBufferSize(single_page_size);
                writer->setPageRange(page_numbers);
            });
            pool.joinAll();
            stop_watch.stop();
        }

        restore_stop_watch.start();
        // restore
        initPageStorage(config, name());
        restore_stop_watch.stop();
    }

private:
    Stopwatch restore_stop_watch;
};

REGISTER_WORKLOAD(HeavyRestore)