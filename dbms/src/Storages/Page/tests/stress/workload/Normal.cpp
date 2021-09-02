#include "../PSWorkload.h"

class NormalWorkload : public StressWorkload
    , public StressWorkloadFunc<NormalWorkload>
{
public:
    static UInt64 mask()
    {
        return NORMAL_WORKLOAD;
    }

private:
    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        LOG_INFO(StressEnv::logger, "Start Running WorkLoad-Normal");

        DB::PageStorage::Config config;
        config.num_write_slots = options.num_writer_slots;
        initPageStorage(config);

        if (options.avg_page_size_mb != 0)
        {
            PSWriter::setApproxPageSize(options.avg_page_size_mb);
        }

        // init all pages in PageStorage
        if (options.init_pages)
        {
            PSWriter::fillAllPages(ps);
            LOG_INFO(StressEnv::logger, "All pages have been init.");
        }

        stop_watch.start();

        startWriter<PSWriter>(options.num_writers);
        startReader(options.num_readers);
        startBackgroundTimer();

        pool.joinAll();
        stop_watch.stop();
    }
};

REGISTER_WORKLOAD(NormalWorkload)