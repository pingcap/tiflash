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
class HeavyWrite
    : public StressWorkload
    , public StressWorkloadFunc<HeavyWrite>
{
public:
    explicit HeavyWrite(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "HeavyWrite"; }

    static UInt64 mask() { return 1 << 2; }

private:
    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored"
            "`paths` will only used first one. which is {}. Data will store in {}"
            "Please cleanup folder after this test."
            "The current workload will elapse near 60 seconds",
            options.paths[0],
            options.paths[0] + "/" + name());
    }

    void run() override
    {
        DB::PageStorageConfig config;
        initPageStorage(config, name());

        startBackgroundTimer();
        {
            stop_watch.start();
            startWriter<PSCommonWriter>(options.num_writers, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBufferSizeRange(1, 2 * DB::MB);
            });

            pool.joinAll();
            stop_watch.stop();
        }
    }

    bool verify() override { return true; }
};
} // namespace DB::PS::tests
