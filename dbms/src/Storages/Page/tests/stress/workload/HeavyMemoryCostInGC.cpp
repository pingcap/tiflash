#include <PSWorkload.h>

class HeavyMemoryCostInGC
    : public StressWorkload
    , public StressWorkloadFunc<HeavyMemoryCostInGC>
{
public:
    explicit HeavyMemoryCostInGC(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name()
    {
        return "HeavyMemoryCostInGCWorkload";
    }

    static UInt64 mask()
    {
        return 1 << 1;
    }

    String desc() override
    {
        return fmt::format("Some of options will be ignored"
                           "`paths` will only used first one. which is {}. Data will store in {}"
                           "Please cleanup folder after this test."
                           "The current workload will elapse near 30 seconds, and GC will be performed at the end.",
                           options.paths[0],
                           options.paths[0] + "/" + name());
    }

    void run() override
    {
        stop_watch.start();

        DB::PageStorage::Config config;
        initPageStorage(config, name());

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        stress_time = std::make_shared<StressTimeout>(30);
        stress_time->start();

        startWriter<PSCommonWriter>(options.num_writers, [](std::shared_ptr<PSCommonWriter> writer) -> void {
            writer->setBatchBufferNums(100);
            writer->setBatchBufferSize(1);
        });

        pool.joinAll();
        stop_watch.stop();

        gc = std::make_shared<PSGc>(ps);
        gc->doGcOnce();
    }

    bool verify() override
    {
        return (metrics_dumper->getMemoryPeak() < 5UL * 1024 * 1024);
    }

    void onFailed() override
    {
        LOG_WARNING(StressEnv::logger,
                    fmt::format("Memory Peak is {} , it should not bigger than {} ", metrics_dumper->getMemoryPeak(), 5 * 1024 * 1024));
    }
};

REGISTER_WORKLOAD(HeavyMemoryCostInGC)
