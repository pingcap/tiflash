#include <PSWorkload.h>
#include <sys/resource.h>

class PageStorageInMemoryCapacity : public StressWorkload
    , public StressWorkloadFunc<PageStorageInMemoryCapacity>
{
public:
    explicit PageStorageInMemoryCapacity(const StressEnv & options_)
        : StressWorkload(options_)
    {}

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
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        DB::PageStorage::Config config;
        initPageStorage(config, name());

        metrics_dumper = std::make_shared<PSMetricsDumper>(1);
        metrics_dumper->start();

        stress_time = std::make_shared<StressTimeout>(60);
        stress_time->start();
        {
            stop_watch.start();
            startWriter<PSIncreaseWriter>(options.num_writers, [](std::shared_ptr<PSIncreaseWriter> writer) -> void {
                writer->setBatchBufferNums(1);
                writer->setBatchBufferSize(10 * 1024);
                writer->setPageRange(50000);
            });

            pool.joinAll();
            stop_watch.stop();
        }

        // todo
    }

    bool verify() override
    {
        return true;
    }
};

REGISTER_WORKLOAD(PageStorageInMemoryCapacity)