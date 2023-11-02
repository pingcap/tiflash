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

#include <Common/Exception.h>
#include <Storages/Page/workload/PSRunnable.h>
#include <Storages/Page/workload/PSWorkload.h>

namespace DB::PS::tests
{
class NormalWorkload
    : public StressWorkload
    , public StressWorkloadFunc<NormalWorkload>
{
public:
    explicit NormalWorkload(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "Normal workload"; }

    static UInt64 mask() { return NORMAL_WORKLOAD; }

    String desc() override { return options.toDebugString(); }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);

        DB::PageStorageConfig config;
        config.num_write_slots = options.num_writer_slots;
        initPageStorage(config);

        // init all pages in PageStorage
        if (options.init_pages)
        {
            static constexpr PageIdU64 MAX_PAGE_ID_DEFAULT = 1000;
            initPages(MAX_PAGE_ID_DEFAULT);
            LOG_INFO(options.logger, "All pages have been init.");
        }

        RUNTIME_CHECK(options.avg_page_size > 1000);

        stop_watch.start();

        startWriter<PSWriter>(options.num_writers, [this](std::shared_ptr<PSWriter> w) {
            // A small random range
            w->setBufferSizeRange(options.avg_page_size - 1000 / 2, options.avg_page_size + 1000 / 2);
        });
        const size_t read_delay_ms = options.read_delay_ms;
        startReader<PSReader>(options.num_readers, [read_delay_ms](std::shared_ptr<PSReader> reader) -> void {
            reader->setReadDelay(read_delay_ms);
        });
        startBackgroundTimer();

        pool.joinAll();
        stop_watch.stop();
    }
};
} // namespace DB::PS::tests
