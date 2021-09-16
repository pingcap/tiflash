#include <PSWorkload.h>

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
    }

    bool verify() override
    {
        return true;
    }
};

REGISTER_WORKLOAD(PageStorageInMemoryCapacity)