#include <PSWorkload.h>

class NormalWorkload
    : public StressWorkload
    , public StressWorkloadFunc<NormalWorkload>
{
public:
    explicit NormalWorkload(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name()
    {
        return "Normal workload";
    }

    static UInt64 mask()
    {
        return NORMAL_WORKLOAD;
    }

    String desc() override
    {
        return options.toDebugString();
    }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);

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
        const size_t read_delay_ms = options.read_delay_ms;
        startReader<PSReader>(options.num_readers, [read_delay_ms](std::shared_ptr<PSReader> reader) -> void {
            reader->setReadDelay(read_delay_ms);
        });
        startBackgroundTimer();

        pool.joinAll();
        stop_watch.stop();
    }
};

REGISTER_WORKLOAD(NormalWorkload)
