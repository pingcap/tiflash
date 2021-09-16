#include <PSWorkload.h>

class HeavyRead : public StressWorkload
    , public StressWorkloadFunc<HeavyRead>
{
public:
    explicit HeavyRead(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name()
    {
        return "HeavyRead";
    }

    static UInt64 mask()
    {
        return 1 << 3;
    }

private:
    String desc() override
    {
        return fmt::format("Some of options will be ignored"
                           "`paths` will only used first one. which is {}. Data will store in {}"
                           "Please cleanup folder after this test."
                           "The current workload will elapse near 60 seconds",
                           options.paths[0],
                           options.paths[0] + "/" + name());
    }

    void run() override
    {
        DB::PageStorage::Config config;
        initPageStorage(config, name());
        PSWriter::fillAllPages(ps);

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        stress_time = std::make_shared<StressTimeout>(60);
        stress_time->start();
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

REGISTER_WORKLOAD(HeavyRead)