// Copyright 2024 PingCAP, Inc.
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
#include <Storages/Page/workload/PSStressEnv.h>
#include <Storages/Page/workload/PSWorkload.h>

#include <magic_enum.hpp>

namespace DB::PS::tests
{
class EmptyPages
    : public StressWorkload
    , public StressWorkloadFunc<EmptyPages>
{
public:
    explicit EmptyPages(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "EmptyPagesWorkload"; }
    static UInt64 mask() { return 1 << 8; }

    String desc() override { return fmt::format("write with empty pages"); }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        DB::PageStorageConfig config;

        initPageStorage(config, name());

        startBackgroundTimer();

        // read/write some normal pages
        LOG_INFO(Logger::get(), "Start to generate some pages");
        {
            stop_watch.start();
            const auto num_writers = options.num_writers;
            startWriter<PSWindowWriter>(num_writers, [&](std::shared_ptr<PSWindowWriter> writer) {
                if (writer->id() == 0) {
                    writer->setBatchBufferNums(1);
                    writer->setBufferSizeRange(0, 0);
                    writer->setNormalDistributionSigma(250);
                }
                else
                {
                    writer->setBatchBufferNums(1);
                    writer->setBufferSizeRange(0, options.avg_page_size * 2);
                    writer->setNormalDistributionSigma(250);
                }
            });

            startReader<PSWindowReader>(options.num_readers, [](std::shared_ptr<PSWindowReader> reader) {
                reader->setReadPageNums(5);
                reader->setReadDelay(0);
                reader->setNormalDistributionSigma(250);
            });

            pool.joinAll();
            stop_watch.stop();
        }

        if (StressEnvStatus::getInstance().statCode() < 0)
        {
            LOG_ERROR(
                Logger::get(),
                "Something wrong happen! stat={}",
                magic_enum::enum_name(StressEnvStatus::getInstance().getStat()));
            return;
        }

        // restart
        LOG_INFO(Logger::get(), "Reopen the PageStorage instance");
        initPageStorage(config, name());
    }

    bool verify() override { return true; }

    void onFailed() override {}
};
} // namespace DB::PS::tests
