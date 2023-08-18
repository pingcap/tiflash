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

#include <Storages/Page/workload/PSStressEnv.h>
#include <Storages/Page/workload/PSWorkload.h>

namespace DB::PS::tests
{
class HeavySkewWriteRead
    : public StressWorkload
    , public StressWorkloadFunc<HeavySkewWriteRead>
{
public:
    explicit HeavySkewWriteRead(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "HeavySkewWriteRead"; }

    static UInt64 mask() { return 1 << 4; }

private:
    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored"
            "`paths` will only used first one. which is {}. Data will store in {}. "
            "Please cleanup folder after this test. "
            "The current workload will elapse near 60 seconds",
            options.paths[0],
            options.paths[0] + "/" + name());
    }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        DB::PageStorageConfig config;
        initPageStorage(config, name());

        startBackgroundTimer();

        {
            stop_watch.start();
            const auto num_writers = options.num_writers;
            startWriter<PSWindowWriter>(num_writers, [&](std::shared_ptr<PSWindowWriter> writer) {
                writer->setBatchBufferNums(1);
                writer->setBufferSizeRange(0, options.avg_page_size * 2);
                writer->setNormalDistributionSigma(250);
            });

            startReader<PSWindowReader>(options.num_readers, [](std::shared_ptr<PSWindowReader> reader) {
                reader->setReadPageNums(5);
                reader->setReadDelay(0);
                reader->setNormalDistributionSigma(250);
            });

            pool.joinAll();
            stop_watch.stop();
        }
    }

    bool verify() override { return true; }
};
} // namespace DB::PS::tests
