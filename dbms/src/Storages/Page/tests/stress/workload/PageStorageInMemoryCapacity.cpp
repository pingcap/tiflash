#include <PSWorkload.h>

class PageStorageInMemoryCapacity : public StressWorkload
    , public StressWorkloadFunc<PageStorageInMemoryCapacity>
{
public:
    static String name()
    {
        return "PageStorageInMemoryCapacity";
    }

    static UInt64 mask()
    {
        return 1 << 7;
    }

private:
    String desc() override
    {
        return fmt::format("Some of options will be ignored"
                           "`paths` will only used first one. which is {}. Data will store in {}"
                           "Please cleanup folder after this test."
                           "The current workload will measure the capacity of Pagestorage.",
                           options.paths[0],
                           options.paths[0] + "/" + name());
    }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers);
        DB::PageStorage::Config config;
        initPageStorage(config, name());

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        // 90-100 snapshots will be generated.
        {
            stop_watch.start();
            startWriter<PSWindowWriter>(options.num_writers, [](std::shared_ptr<PSWindowWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBatchBufferSize(10 * 1024);
                writer->setBatchBufferLimit(1ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
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

REGISTER_WORKLOAD(PageStorageInMemoryCapacity)