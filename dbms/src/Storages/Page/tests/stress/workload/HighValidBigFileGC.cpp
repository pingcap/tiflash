#include "../PSWorkload.h"

class HighValidBigFileGCWorkload : public StressWorkload
    , public StressWorkloadFunc<HighValidBigFileGCWorkload>
{
public:
    static String name()
    {
        return "HighValidBigPageFileGCWorkload";
    }

    static UInt64 mask()
    {
        return 0x1;
    }

private:
    String desc() override
    {
        return fmt::format(" Some of options will be ignored"
                           "`paths` will only used first one. which is {}. Data will store in {}"
                           "Please cleanup folder after this test."
                           "The current workload will generate 9G data, and GC will be performed at the end.",
                           options.paths[0],
                           options.paths[0] + "/" + name());
    }

    void run() override
    {
        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        // For safe , setup timeout.
        stress_time = std::make_shared<StressTimeout>(100);
        stress_time->start();

        // Generate 8G data in the same Pagefile
        {
            stop_watch.start();

            DB::PageStorage::Config config;
            config.file_max_size = 8ULL * DB::GB;
            config.file_roll_size = 8ULL * DB::GB;
            initPageStorage(config, name());

            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBatchBufferSize(100ULL * DB::MB);
                writer->setBatchBufferLimit(8ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
            });

            pool.joinAll();
            stop_watch.stop();
            result();
        }

        LOG_INFO(StressEnv::logger, "Already generator A 8G page file");

        // Generate normal data in the same Pagefile
        {
            stop_watch.start();
            DB::PageStorage::Config config;
            config.file_max_size = DB::PAGE_FILE_MAX_SIZE;
            config.file_roll_size = DB::PAGE_FILE_ROLL_SIZE;
            initPageStorage(config, name());
            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBatchBufferSize(2ULL * DB::MB);
                writer->setBatchBufferLimit(1ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
            });

            pool.joinAll();
            stop_watch.stop();
            result();
        }

        // TBD : maybe change back , invoke gc ?

        gc = std::make_shared<PSGc>(ps);
        gc->doGcOnce();

        {
            stop_watch.start();
            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBatchBufferSize(2ULL * DB::MB);
                writer->setBatchBufferLimit(1ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
            });

            pool.joinAll();
            stop_watch.stop();
            result();
        }

        gc->doGcOnce();
    }

    bool verify() override
    {
        if ()
            return true;
    };

    void failed() override{
        // TBD
    };
};

REGISTER_WORKLOAD(HighValidBigFileGCWorkload)