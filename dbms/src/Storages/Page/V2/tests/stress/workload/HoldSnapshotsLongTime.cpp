#include <PSWorkload.h>

class HoldSnapshotsLongTime : public StressWorkload
    , public StressWorkloadFunc<HoldSnapshotsLongTime>
{
public:
    explicit HoldSnapshotsLongTime(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name()
    {
        return "HoldSnapshotsLongTime";
    }

    static UInt64 mask()
    {
        return 1 << 6;
    }

private:
    String desc() override
    {
        return fmt::format("Some of options will be ignored"
                           "`paths` will only used first one. which is {}. Data will store in {}"
                           "Please cleanup folder after this test."
                           "The current workload will elapse near 60 seconds and generator 100 snapshot in memory."
                           "Then do NORMAL GC + SKIP GC at the last.",
                           options.paths[0],
                           options.paths[0] + "/" + name());
    }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        PageStorage::Config config;
        initPageStorage(config, name());

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        stress_time = std::make_shared<StressTimeout>(60);
        stress_time->start();

        scanner = std::make_shared<PSScanner>(ps);
        scanner->start();

        // 90-100 snapshots will be generated.
        {
            stop_watch.start();
            startWriter<PSWindowWriter>(options.num_writers, [](std::shared_ptr<PSWindowWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBatchBufferRange(10 * 1024, 1 * DB::MB);
                writer->setWindowSize(500);
                writer->setNormalDistributionSigma(13);
            });

            startReader<PSSnapshotReader>(1, [](std::shared_ptr<PSSnapshotReader> reader) -> void {
                reader->setSnapshotGetIntervalMs(600);
            });

            pool.joinAll();
            stop_watch.stop();
        }

        gc = std::make_shared<PSGc>(ps);
        // Normal GC
        gc->doGcOnce();

        readers.clear();

        // Skip GC
        gc->doGcOnce();
    }

    bool verify() override
    {
        return true;
    }
};

REGISTER_WORKLOAD(HoldSnapshotsLongTime)