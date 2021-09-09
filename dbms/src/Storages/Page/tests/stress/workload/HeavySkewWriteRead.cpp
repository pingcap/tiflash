#include <PSWorkload.h>

class HeavySkewWrite : public StressWorkload
    , public StressWorkloadFunc<HeavySkewWrite>
{
public:
    static String name()
    {
        return "HeavySkewWrite";
    }

    static UInt64 mask()
    {
        return 1 << 4;
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
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        DB::PageStorage::Config config;
        initPageStorage(config, name());

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        stress_time = std::make_shared<StressTimeout>(60);
        stress_time->start();
        {
            stop_watch.start();
            startWriter<PSWindowWriter>(options.num_writers, [](std::shared_ptr<PSWindowWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBatchBufferRange(10 * 1024, 1 * DB::MB);
                writer->setWindowSize(500);
                writer->setNormalDistributionSigma(13);
            });

            auto num_writers = options.num_writers;

            startReader<PSWindowReader>(8, [num_writers](std::shared_ptr<PSWindowReader> reader) -> void {
                reader->setPageReadOnce(5);
                reader->setReadDelay(0);
                reader->setWriterNums(num_writers);
                reader->setWindowSize(100);
                reader->setNormalDistributionSigma(9);
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

REGISTER_WORKLOAD(HeavySkewWrite)