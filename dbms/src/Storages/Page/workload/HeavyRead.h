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

#include <Storages/Page/workload/PSRunnable.h>
#include <Storages/Page/workload/PSWorkload.h>

namespace DB::PS::tests
{
class HeavyRead
    : public StressWorkload
    , public StressWorkloadFunc<HeavyRead>
{
public:
    explicit HeavyRead(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "HeavyRead"; }

    static UInt64 mask() { return 1 << 3; }

private:
    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored"
            "`paths` will only used first one. which is {}. Data will store in {}"
            "Please cleanup folder after this test."
            "The current workload will force init page in {} and it elapse near 60 seconds",
            options.paths[0],
            options.paths[0] + "/" + name(),
            options.paths[0] + "/" + name());
    }

    void run() override
    {
        DB::PageStorageConfig config;
        initPageStorage(config, name());
        initPages(MAX_PAGE_ID_DEFAULT);

        startBackgroundTimer();
        {
            stop_watch.start();
            startReader<PSReader>(options.num_readers, [](std::shared_ptr<PSReader> reader) -> void {
                // No delay
                reader->setReadDelay(0);
                reader->setReadPageRange(MAX_PAGE_ID_DEFAULT);
                reader->setReadPageNums(10);
            });
            pool.joinAll();
            stop_watch.stop();
        }
    }
};
} // namespace DB::PS::tests
