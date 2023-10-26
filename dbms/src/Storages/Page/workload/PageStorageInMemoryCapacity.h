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
#include <fcntl.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <unistd.h>

#ifdef __APPLE__

#include <libproc.h>
#include <mach/mach_host.h>
#include <sys/proc_info.h>
#include <sys/sysctl.h>
#endif

namespace DB::PS::tests
{
class PageStorageInMemoryCapacity
    : public StressWorkload
    , public StressWorkloadFunc<PageStorageInMemoryCapacity>
{
public:
    explicit PageStorageInMemoryCapacity(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "PageStorageInMemoryCapacity"; }

    static UInt64 mask() { return 1 << 7; }

    static std::tuple<UInt64, UInt64, UInt64> getCurrentMemory()
    {
        UInt64 virtual_size = 0;
        UInt64 resident_size = 0;
        UInt64 total_mem = 0;
#ifdef __APPLE__
        struct proc_taskinfo pti;
        if (sizeof(pti) == proc_pidinfo(getpid(), PROC_PIDTASKINFO, 0, &pti, sizeof(pti)))
        {
            virtual_size = pti.pti_virtual_size;
            resident_size = pti.pti_resident_size;
        }
        mach_msg_type_number_t info_size = HOST_BASIC_INFO_COUNT;
        host_basic_info_data_t info_data;

        kern_return_t kern_rc
            = host_info(mach_host_self(), HOST_BASIC_INFO, reinterpret_cast<host_info_t>(&info_data), &info_size);
        if (kern_rc == KERN_SUCCESS)
        {
            total_mem = info_data.max_mem;
        }

#elif __linux__
#define PAGE_SIZE (sysconf(_SC_PAGESIZE))
        int fd = open("/proc/self/statm", O_RDONLY);
        if (fd != -1)
        {
            size_t max_proc_line = 4096 + 1;
            char buf[max_proc_line];
            ssize_t rres = read(fd, buf, max_proc_line);

            close(fd);
            if (rres > 0)
            {
                char * pos = buf;
                virtual_size = strtol(pos, &pos, 10);
                if (*pos == ' ')
                    pos++;
                resident_size = strtol(pos, &pos, 10);
                if (*pos == ' ')
                    pos++;
            }
        }

        FILE * file = fopen("/proc/meminfo", "r");
        if (file != nullptr)
        {
            char buffer[128];
#define MEMORY_TOTAL_LABEL "MemTotal:"
            while (fgets(buffer, 128, file))
            {
                if ((strncmp((buffer), (MEMORY_TOTAL_LABEL), strlen(MEMORY_TOTAL_LABEL)) == 0)
                    && sscanf(buffer + strlen(MEMORY_TOTAL_LABEL), " %32lu kB", &total_mem)) // NOLINT
                {
                    break;
                }
            }
#undef MEMORY_TOTAL_LABEL
        }
        total_mem = total_mem * 1024;
        virtual_size = virtual_size * PAGE_SIZE;
        resident_size = resident_size * PAGE_SIZE;
#endif
        return std::make_tuple(total_mem, virtual_size, resident_size);
    }


private:
    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored"
            "`paths` will only used first one. which is {}. Data will store in {}"
            "Please cleanup folder after this test."
            "The current workload will measure the capacity of Pagestorage.",
            options.paths[0],
            options.paths[0] + "/" + name());
    }

    void run() override
    {
        const size_t single_writer_page_nums = 250000;
        UInt64 begin_virtual_size = 0;
        UInt64 begin_resident_size = 0;
        UInt64 end_virtual_size = 0;
        UInt64 end_resident_size = 0;
        UInt64 total_mem = 0;

        std::tie(total_mem, begin_virtual_size, begin_resident_size) = getCurrentMemory();
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        DB::PageStorageConfig config;
        initPageStorage(config, name());

        startBackgroundTimer();

        {
            stop_watch.start();
            startWriter<PSIncreaseWriter>(options.num_writers, [](std::shared_ptr<PSIncreaseWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBufferSizeRange(10ULL * 1024, 10ULL * 1024);
                writer->setPageRange(single_writer_page_nums);
            });

            pool.joinAll();
            stop_watch.stop();
        }

        std::tie(total_mem, end_virtual_size, end_resident_size) = getCurrentMemory();

        size_t virtual_used = (end_virtual_size - begin_virtual_size);
        size_t resident_used = (end_resident_size - begin_resident_size);
        size_t page_writen = (single_writer_page_nums * options.num_writers);
        assert(page_writen != 0);

        LOG_INFO(
            options.logger,
            "After gen: {} pages"
            "virtual memory used: {} MB,"
            "resident memory used: {} MB,"
            "total memory is {} , It is estimated that {} pages can be stored in the virtual memory,"
            "It is estimated that {} pages can be stored in the resident memory.",
            page_writen,
            virtual_used / DB::MB,
            resident_used / DB::MB,
            total_mem,
            std::round(virtual_used) ? (total_mem / ((double)virtual_used / page_writen)) : 0,
            std::round(resident_used) ? (total_mem / ((double)resident_used / page_writen)) : 0);
    }
};
} // namespace DB::PS::tests
