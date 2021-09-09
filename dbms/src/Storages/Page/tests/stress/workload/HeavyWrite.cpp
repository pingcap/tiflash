#include <PSWorkload.h>

class HeavyWrite : public StressWorkload
    , public StressWorkloadFunc<HeavyWrite>
{
public:
    static String name()
    {
        return "HeavyWrite";
    }

    static UInt64 mask()
    {
        return 1 << 2;
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

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        stress_time = std::make_shared<StressTimeout>(60);
        stress_time->start();
        {
            stop_watch.start();
            startWriter<PSCommonWriter>(options.num_writers, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBatchBufferRange(1, 2 * DB::MB);
            });

            pool.joinAll();
            stop_watch.stop();
        }
    }

    bool verify() override
    {
        return true;
    }
};

REGISTER_WORKLOAD(HeavyWrite)