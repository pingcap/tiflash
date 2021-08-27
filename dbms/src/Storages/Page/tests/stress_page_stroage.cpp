#include <Common/FailPoint.h>
#include <Common/MemoryTracker.h>
#include <Common/Stopwatch.h>
#include <Common/UnifiedLogPatternFormatter.h>
#include <Encryption/MockKeyManager.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <TestUtils/MockDiskDelegator.h>
#include <common/logger_useful.h>
#include <fmt/format.h>
#include <signal.h>

#include <atomic>
#include <boost/program_options.hpp>
#include <chrono>
#include <iostream>
#include <list>
#include <memory>
#include <random>

using PSPtr = std::shared_ptr<DB::PageStorage>;
const DB::PageId MAX_PAGE_ID_DEFAULT = 1000;

std::atomic<bool> running_without_exception = true;
std::atomic<bool> running_without_timeout = true;

Poco::Logger * logger = nullptr;

/* some exported global vars */
namespace DB
{
#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif

namespace FailPoints
{
extern const char random_slow_page_storage_remove_expired_snapshots[];
extern const char random_slow_page_storage_list_all_live_files[];
} // namespace FailPoints

} // namespace DB
/* some exported global vars */

struct StressEnv
{
    size_t num_writers = 1;
    size_t num_readers = 4;
    bool init_pages = false;
    bool clean_before_run = false;
    size_t timeout_s = 0;
    size_t read_delay_ms = 0;
    size_t num_writer_slots = 1;
    size_t avg_page_size_mb = 1;
    size_t rand_seed = 0x123987;
    size_t status_interval = 1;
    size_t situation_mask = 0;

    std::vector<std::string> paths;
    std::vector<std::string> failpoints;

    std::string toDebugString() const
    {
        return fmt::format("{{ num_writers: {}, num_readers: {}, clean_before_run: {}" //
                           ", timeout_s: {}, read_delay_ms: {}, num_writer_slots: {}"
                           ", avg_page_size_mb: {}, rand_seed: {:08x} paths: [{}] failpoints: [{}] }}"
                           ", status_interval: {}, situation_mask : {}",
                           num_writers,
                           num_readers,
                           clean_before_run,
                           timeout_s,
                           read_delay_ms,
                           num_writer_slots,
                           avg_page_size_mb,
                           rand_seed,
                           fmt::join(paths.begin(), paths.end(), ","),
                           fmt::join(failpoints.begin(), failpoints.end(), ","),
                           status_interval,
                           situation_mask
                           //
        );
    }

    static void initGlobalLogger()
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new DB::UnifiedLogPatternFormatter);
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel("trace");
        logger = &Poco::Logger::get("root");
    }

    // <prog> -W 4 -R 128 -T 10 -C 1 --paths ./stress1 --paths ./stress2
    static StressEnv parse(int argc, char ** argv)
    {
        namespace po = boost::program_options;
        using po::value;
        po::options_description desc("Allowed options");
        desc.add_options()("help,h", "produce help message") //
            ("write_concurrency,W", value<UInt32>()->default_value(4), "number of write threads") //
            ("read_concurrency,R", value<UInt32>()->default_value(16), "number of read threads") //
            ("clean_before_run,C", value<bool>()->default_value(false), "drop data before running") //
            ("init_pages,I", value<bool>()->default_value(false), "init pages if not exist before running") //
            ("timeout,T", value<UInt32>()->default_value(600), "maximum run time (seconds). 0 means run infinitely") //
            ("writer_slots", value<UInt32>()->default_value(4), "number of PageStorage writer slots") //
            ("read_delay_ms", value<UInt32>()->default_value(0), "millionseconds of read delay") //
            ("avg_page_size", value<UInt32>()->default_value(1), "avg size for each page(MiB)") //
            ("rand_seed", value<UInt32>()->default_value(0x123987), "random seed") //
            ("paths,P", value<std::vector<std::string>>(), "store path(s)") //
            ("failpoints,F", value<std::vector<std::string>>(), "failpoint(s) to enable") //
            ("status_interval,S", value<UInt32>()->default_value(1), "Status statistics interval. 0 means no statistics") //
            ("situation_mask,M", value<UInt64>()->default_value(0), "Run special tests sequentially,example -M 0x2"); //

        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);
        po::notify(options);

        if (options.count("help") > 0)
        {
            std::cerr << desc << std::endl;
            exit(0);
        }

        StressEnv opt;
        opt.num_writers = options["write_concurrency"].as<UInt32>();
        opt.num_readers = options["read_concurrency"].as<UInt32>();
        opt.init_pages = options["init_pages"].as<bool>();
        opt.clean_before_run = options["clean_before_run"].as<bool>();
        opt.timeout_s = options["timeout"].as<UInt32>();
        opt.read_delay_ms = options["read_delay_ms"].as<UInt32>();
        opt.num_writer_slots = options["writer_slots"].as<UInt32>();
        opt.avg_page_size_mb = options["avg_page_size"].as<UInt32>();
        opt.rand_seed = options["rand_seed"].as<UInt32>();
        opt.status_interval = options["status_interval"].as<UInt32>();
        opt.situation_mask = options["situation_mask"].as<UInt64>();

        if (options.count("paths"))
            opt.paths = options["paths"].as<std::vector<std::string>>();
        else
            opt.paths = {"./stress"};

        if (options.count("failpoints"))
            opt.failpoints = options["failpoints"].as<std::vector<std::string>>();
        return opt;
    }

    void setup()
    {
        CurrentMemoryTracker::disableThreshold();
#ifdef FIU_ENABLE
        fiu_init(0);
#endif
        for (const auto & fp : failpoints)
        {
            DB::FailPointHelper::enableFailPoint(fp);
        }

        // set random seed
        srand(rand_seed);

        // drop dir if exists
        bool all_directories_not_exist = true;
        for (const auto & path : paths)
        {
            if (Poco::File file(path); file.exists())
            {
                all_directories_not_exist = false;
                if (clean_before_run)
                {
                    file.remove(true);
                }
            }
        }

        if (clean_before_run)
            LOG_INFO(logger, "All pages have been drop.");

        if (clean_before_run || all_directories_not_exist)
            init_pages = true;

        signal(SIGINT, [](int /*signal*/) {
            LOG_ERROR(logger, "Receive finish signal. Wait for the GC threads to end.\n");
            running_without_timeout = false;
        });
    }
};

class PSRunnable : public Poco::Runnable
{
public:
    size_t bytes_used = 0;
    size_t pages_used = 0;

public:
    void run() override
    {
        MemoryTracker tarcker;
        current_memory_tracker = &tarcker;
        // If runImpl() return false, means it need break itself
        while (running_without_exception && running_without_timeout && runImpl())
        {
            /*Just for no warming*/
        }
        tarcker.setDescription(description().c_str());
        current_memory_tracker = nullptr;
        LOG_INFO(logger, description() + " exit");
    }

    virtual String description() = 0;
    virtual bool runImpl() = 0;
};

class PSWriter : public PSRunnable
{
    static size_t approx_page_mb;

public:
    PSWriter(const PSPtr & ps_, DB::UInt32 index_)
        : PSRunnable()
        , ps(ps_)
        , index(index_)
    {}

    static void setApproxPageSize(size_t size_mb)
    {
        LOG_INFO(logger, fmt::format("Page approx size is set to {} MB", size_mb));
        approx_page_mb = size_mb;
    }

    static DB::ReadBufferPtr genRandomData(const DB::PageId pageId, DB::MemHolder & holder)
    {
        // fill page with random bytes
        const size_t buff_sz = approx_page_mb * DB::MB + random() % 3000;
        char * buff = (char *)malloc(buff_sz);
        const char buff_ch = pageId % 0xFF;
        memset(buff, buff_ch, buff_sz);

        holder = DB::createMemHolder(buff, [&](char * p) { free(p); });

        return std::make_shared<DB::ReadBufferFromMemory>(buff, buff_sz);
    }

    static void fillAllPages(const PSPtr & ps)
    {
        for (DB::PageId pageId = 0; pageId < MAX_PAGE_ID_DEFAULT; ++pageId)
        {
            DB::MemHolder holder;
            DB::ReadBufferPtr buff = genRandomData(pageId, holder);

            DB::WriteBatch wb;
            wb.putPage(pageId, 0, buff, buff->buffer().size());
            ps->write(std::move(wb));
            if (pageId % 100 == 0)
                LOG_INFO(logger, fmt::format("writer wrote page {}", pageId));
        }
    }

    virtual String description() override { return fmt::format("(Stress Test Writer {})", index); }

    virtual bool runImpl() override
    {
        assert(ps != nullptr);
        const DB::PageId pageId = genRandomPageId();

        DB::MemHolder holder;
        DB::ReadBufferPtr buff = genRandomData(pageId, holder);

        DB::WriteBatch wb;
        wb.putPage(pageId, 0, buff, buff->buffer().size());
        ps->write(std::move(wb));
        ++pages_used;
        bytes_used += buff->buffer().size();
        return true;
    }

protected:
    virtual DB::PageId genRandomPageId()
    {
        std::normal_distribution<> distribution{(double)max_page_id / 2, 150};
        return static_cast<DB::PageId>(std::round(distribution(gen))) % max_page_id;
        ;
    }

protected:
    PSPtr ps;
    DB::UInt32 index = 0;
    std::mt19937 gen;
    DB::PageId max_page_id = MAX_PAGE_ID_DEFAULT;
};

// PSCommonWriter can custom data size/numbers/page id range in one writebatch.
// And it also can set max_io_limit,after send limit size data into pagefile. it will stop itself.
class PSCommonWriter : public PSWriter
{
public:
    PSCommonWriter(const PSPtr & ps_, DB::UInt32 index_)
        : PSWriter(ps_, index_)
    {}

    void updatedRandomData()
    {
        buffPtrs.clear();
        for (size_t i = 0; i < batch_buffer_nums; ++i)
        {
            char * buff = (char *)malloc(batch_buffer_size);
            DB::MemHolder holder = DB::createMemHolder(buff, [&](char * p) { free(p); });
            buffPtrs.push_back(std::make_shared<DB::ReadBufferFromMemory>(buff, batch_buffer_size));
        }
    }

    String description() override { return fmt::format("(Stress Test Common Writer {})", index); }

    bool runImpl() override
    {
        assert(ps != nullptr);
        const DB::PageId pageId = genRandomPageId();

        DB::WriteBatch wb;
        updatedRandomData();

        for (auto & buffptr : buffPtrs)
        {
            wb.putPage(pageId, 0, buffptr, batch_buffer_size);
            ++pages_used;
            bytes_used += batch_buffer_size;
        }

        ps->write(std::move(wb));
        return (batch_buffer_limit == 0 || bytes_used < batch_buffer_limit);
    }

    void setBatchBufferNums(size_t numbers) { batch_buffer_nums = numbers; }

    void setBatchBufferSize(size_t size) { batch_buffer_size = size; }

    void setBatchBufferLimit(size_t size_limit) { batch_buffer_limit = size_limit; }

    void setBatchBufferPageRange(size_t max_page_id_) { max_page_id = max_page_id_; }

protected:
    size_t batch_buffer_nums = 100;
    size_t batch_buffer_size = 1 * DB::MB;
    size_t batch_buffer_limit = 0;

    virtual DB::PageId genRandomPageId() override { return static_cast<DB::PageId>(rand() % max_page_id); }

private:
    std::vector<DB::ReadBufferPtr> buffPtrs;
};

size_t PSWriter::approx_page_mb = 2;

class PSReader : public PSRunnable
{
    const size_t heavy_read_delay_ms;

public:
    PSReader(const PSPtr & ps_, DB::UInt32 index_, size_t delay_ms)
        : PSRunnable()
        , heavy_read_delay_ms(delay_ms)
        , ps(ps_)
        , index(index_)
    {}

    bool runImpl() override
    {
        assert(ps != nullptr);
        {
            // sleep [0~10) ms
            const uint32_t micro_seconds_to_sleep = random() % 10;
            usleep(micro_seconds_to_sleep * 1000);
        }
        std::vector<DB::PageId> pageIds;
        for (size_t i = 0; i < 5; ++i)
        {
            pageIds.emplace_back(random() % max_page_id);
        }
        try
        {
            // std::function<void(PageId page_id, const Page &)>;
            DB::PageHandler handler = [&](DB::PageId page_id, const DB::Page & page) {
                (void)page_id;
                // use `sleep` to mock heavy read
                if (heavy_read_delay_ms > 0)
                {
                    //const uint32_t micro_seconds_to_sleep = 10;
                    usleep(heavy_read_delay_ms * 1000);
                }
                ++pages_used;
                bytes_used += page.data.size();
            };
            ps->read(pageIds, handler);
        }
        catch (DB::Exception & e)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        return true;
    }

    String description() override { return fmt::format("(Stress Test PSReader {})", index); }

protected:
    PSPtr ps;
    DB::UInt32 index = 0;
    DB::PageId max_page_id = MAX_PAGE_ID_DEFAULT;
};

class PSMetricsDumper
{
public:
    PSMetricsDumper(size_t status_interval_)
        : status_interval(status_interval_)
    {
        timer_status.setStartInterval(1000);
        timer_status.setPeriodicInterval(status_interval * 1000);
    };

    void onTime(Poco::Timer & /* t */)
    {
        lastest_memory = CurrentMetrics::get(CurrentMetrics::MemoryTracking);
        if (likely(lastest_memory != 0))
        {
            loop_times++;
            memory_summary += lastest_memory;
            memory_biggest = memory_biggest > lastest_memory ? memory_biggest : lastest_memory;
            LOG_INFO(logger, toString());
        }
    }

    String toString()
    {
        return fmt::format(
            "Memory lastest used : {} , avg used : {} , top used {}. \n",
            lastest_memory,
            loop_times == 0 ? 0 : (memory_summary / loop_times),
            memory_biggest);
    }

    void start()
    {
        if (status_interval != 0)
        {
            timer_status.start(Poco::TimerCallback<PSMetricsDumper>(*this, &PSMetricsDumper::onTime));
        }
    }

private:
    size_t status_interval = 0;
    UInt32 loop_times = 0;
    UInt32 memory_summary = 0;
    UInt32 memory_biggest = 0;
    UInt32 lastest_memory = 0;

    Poco::Timer timer_status;
};

class PSGc
{
    PSPtr ps;

public:
    PSGc(const PSPtr & ps_)
        : ps(ps_)
    {
        assert(ps != nullptr);
        gc_timer.setStartInterval(1000);
        gc_timer.setPeriodicInterval(30 * 1000);
    }

    void doGcOnce()
    {
        try
        {
            MemoryTracker tarcker;
            tarcker.setDescription("(Stress Test GC)");
            current_memory_tracker = &tarcker;
            ps->gc();
            current_memory_tracker = nullptr;
        }
        catch (...)
        {
            // if gc throw exception stop the test
            running_without_exception = false;
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }

    void onTime(Poco::Timer & /* t */) { doGcOnce(); }

    void start() { gc_timer.start(Poco::TimerCallback<PSGc>(*this, &PSGc::onTime)); }

private:
    Poco::Timer gc_timer;
};
using PSGcPtr = std::shared_ptr<PSGc>;

class PSScanner
{
    PSPtr ps;

public:
    PSScanner(const PSPtr & ps_)
        : ps(ps_)
    {
        assert(ps != nullptr);

        scanner_timer.setStartInterval(1000);
        scanner_timer.setPeriodicInterval(30 * 1000);
    }

    void onTime(Poco::Timer & /* t*/)
    {
        size_t num_snapshots = 0;
        double oldest_snapshot_seconds = 0.0;
        unsigned oldest_snapshot_thread = 0;
        try
        {
            LOG_INFO(logger, "Scanner start");
            std::tie(num_snapshots, oldest_snapshot_seconds, oldest_snapshot_thread) = ps->getSnapshotsStat();
            LOG_INFO(logger,
                     fmt::format("Scanner get {} snapshots, longest lifetime: {:.3f}s longest from thread: {}",
                                 num_snapshots,
                                 oldest_snapshot_seconds,
                                 oldest_snapshot_thread));
        }
        catch (...)
        {
            // if gc throw exception stop the test
            running_without_exception = false;
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }

    void start() { scanner_timer.start(Poco::TimerCallback<PSScanner>(*this, &PSScanner::onTime)); }

private:
    Poco::Timer scanner_timer;
};
using PSScannerPtr = std::shared_ptr<PSScanner>;

class StressTimeout
{
public:
    StressTimeout(size_t timeout_s)
    {
        running_without_timeout = true;
        LOG_INFO(logger, fmt::format("benchmark timeout: {}s", timeout_s));
        timeout_timer.setStartInterval(timeout_s * 1000);
    };

    void onTime(Poco::Timer & /* t */)
    {
        LOG_INFO(logger, "timeout.");
        running_without_timeout = false;
    }

    void start() { timeout_timer.start(Poco::TimerCallback<StressTimeout>(*this, &StressTimeout::onTime)); }

private:
    Poco::Timer timeout_timer;
};
using StressTimeoutPtr = std::shared_ptr<StressTimeout>;

class StressWorkload
{
public:
    StressWorkload(StressEnv & options_)
        : options(options_)
        , pool(/* minCapacity= */ 1 + options.num_writers + options.num_readers, 1 + options.num_writers + options.num_readers)
        , metrics_dumper(options.status_interval)
    {
    }

    void resultWorkload()
    {
        UInt64 timeInterval = stop_watch.elapsedMilliseconds();
        fmt::print(stderr, "result in {}ms\n", timeInterval);
        double seconds_run = 1.0 * timeInterval / 1000;

        size_t total_pages_written = 0;
        size_t total_bytes_written = 0;

        for (auto & writer : writers)
        {
            total_pages_written += writer->pages_used;
            total_bytes_written += writer->bytes_used;
        }

        size_t total_pages_read = 0;
        size_t total_bytes_read = 0;

        for (auto & reader : readers)
        {
            total_pages_read += reader->pages_used;
            total_bytes_read += reader->bytes_used;
        }

        fmt::print(stderr,
                   "W: {} pages, {:.4f} GB, {:.4f} GB/s\n", //
                   total_pages_written,
                   (double)total_bytes_written / DB::GB,
                   (double)total_bytes_written / DB::GB / seconds_run);
        fmt::print(stderr,
                   "R: {} pages, {:.4f} GB, {:.4f} GB/s\n", //
                   total_pages_read,
                   (double)total_bytes_read / DB::GB,
                   (double)total_bytes_read / DB::GB / seconds_run);

        if (options.status_interval != 0)
        {
            fmt::print(stderr, metrics_dumper.toString());
        }
    }

    static void runNormalWorkload(StressEnv & options_) { StressWorkload(options_).normalWorkload(); }

    static void runPageFileUpdateLongTimeWorkload(StressEnv & options_) { StressWorkload(options_).pageFileUpdateLongTimeWorkload(); }

    static void runHeavyCostInLegacyCompactWorkload(StressEnv & options_) { StressWorkload(options_).heavyCostInLegacyCompactWorkload(); }

private:
    void normalWorkload()
    {
        LOG_INFO(logger, "Start Running WorkLoad-Normal");

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
            LOG_INFO(logger, "All pages have been init.");
        }

        startWriter<PSWriter>(options.num_writers);
        startReader(options.num_readers);
        startBackgroundTimer();

        stop_watch.start();
        pool.joinAll();
        stop_watch.stop();
        resultWorkload();
    }

    void pageFileUpdateLongTimeWorkload()
    {
        const String name = "PageFileUpdateLongTime";
        LOG_INFO(logger,
                 fmt::format("Start Running WorkLoad-{}, Some of options will be ignored"
                             "`paths` will only used first one. which is {}. Data will store in {}"
                             "Please cleanup folder after this test."
                             "The current workload will generate 9G data, and GC will be performed at the end.",
                             name,
                             options.paths[0],
                             options.paths[0] + "/" + name));

        metrics_dumper.start();

        // For safe , setup timeout.
        stress_time = std::make_shared<StressTimeout>(100);
        stress_time->start();

        // Generate 8G data in the same Pagefile
        {
            stop_watch.start();

            DB::PageStorage::Config config;
            config.file_max_size = 8ULL * DB::GB;
            config.file_roll_size = 8ULL * DB::GB;
            initPageStorage(config, name);

            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBatchBufferSize(100ULL * DB::MB);
                writer->setBatchBufferLimit(8ULL * DB::GB);
                writer->setBatchBufferPageRange(1000000);
            });

            pool.joinAll();
            stop_watch.stop();
            resultWorkload();
        }

        LOG_INFO(logger, "Already generator A 8G page file");

        // Generate normal data in the same Pagefile
        {
            stop_watch.start();
            DB::PageStorage::Config config;
            config.file_max_size = DB::PAGE_FILE_MAX_SIZE;
            config.file_roll_size = DB::PAGE_FILE_ROLL_SIZE;
            initPageStorage(config, name);
            startWriter<PSCommonWriter>(1, [](std::shared_ptr<PSCommonWriter> writer) -> void {
                writer->setBatchBufferNums(4);
                writer->setBatchBufferSize(2ULL * DB::MB);
                writer->setBatchBufferLimit(1ULL * DB::GB);
            });

            pool.joinAll();
            stop_watch.stop();
            resultWorkload();
        }

        gc = std::make_shared<PSGc>(ps);
        gc->doGcOnce();
    }

    void heavyCostInLegacyCompactWorkload()
    {
        const String name = "HeavyCostInLegacyCompact";
        LOG_WARNING(logger,
                    fmt::format("Start Running WorkLoad-{}, Some of options will be ignored"
                                "`paths` will only used first one. which is {}. Data will store in {}"
                                "Please cleanup folder after this test."
                                "The current workload will elapse near 30 seconds, and GC will be performed at the end.",
                                name,
                                options.paths[0],
                                options.paths[0] + "/" + name));
        stop_watch.start();

        DB::PageStorage::Config config;
        initPageStorage(config, name);

        metrics_dumper.start();

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
        resultWorkload();
    }

    void initPageStorage(DB::PageStorage::Config & config, String path_prefix = "")
    {
        DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);

        if (path_prefix.size() == 0)
        {
            // FIXME: running with `MockDiskDelegatorMulti` is not well-testing
            if (options.paths.empty())
                throw DB::Exception("Can not run without paths");
            if (options.paths.size() == 1)
                delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0]);
            else
                delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(options.paths);
        }
        else
        {
            // Running Special test use this path
            delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0] + "/" + path_prefix);
        }

        ps = std::make_shared<DB::PageStorage>("stress_test", delegator, config, file_provider);
        ps->restore();
        {
            size_t num_of_pages = 0;
            ps->traverse([&num_of_pages](const DB::Page & page) {
                (void)page;
                num_of_pages++;
            });
            LOG_INFO(logger, fmt::format("Recover {} pages.", num_of_pages));
        }
    }

    void startBackgroundTimer()
    {
        gc = std::make_shared<PSGc>(ps);
        gc->start();

        scanner = std::make_shared<PSScanner>(ps);
        scanner->start();

        metrics_dumper.start();
        if (options.timeout_s > 0)
        {
            stress_time = std::make_shared<StressTimeout>(options.timeout_s);
            stress_time->start();
        }
    }

    template <typename T>
    void startWriter(size_t nums_writers, std::function<void(std::shared_ptr<T>)> writer_configure = nullptr)
    {
        writers.clear();
        for (size_t i = 0; i < nums_writers; ++i)
        {
            auto writer = std::make_shared<T>(ps, i);
            if (writer_configure)
            {
                writer_configure(writer);
            }
            writers.insert(writers.end(), writer);
            pool.start(*writer, "writer" + DB::toString(i));
        }
    }

    void startReader(size_t nums_readers)
    {
        readers.clear();
        for (size_t i = 0; i < nums_readers; ++i)
        {
            auto reader = std::make_shared<PSReader>(ps, i, options.read_delay_ms);
            readers.insert(readers.end(), reader);
            pool.start(*reader, "reader" + DB::toString(i));
        }
    }


private:
    StressEnv options;
    Poco::ThreadPool pool;
    PSPtr ps;
    DB::PSDiskDelegatorPtr delegator;

    std::list<std::shared_ptr<PSRunnable>> writers;
    std::list<std::shared_ptr<PSRunnable>> readers;

    Stopwatch stop_watch;

    StressTimeoutPtr stress_time;
    PSScannerPtr scanner;
    PSGcPtr gc;
    PSMetricsDumper metrics_dumper;
};


class StressWorkloadManger
{
public:
    StressWorkloadManger(StressEnv options_)
        : options(options_)
    {}

    void runWorkload()
    {
        if (options.situation_mask >= workload_end)
        {
            LOG_WARNING(logger,
                        fmt::format("situation_mask will be ignore," //
                                    "it should not GE {} ,now is {}",
                                    workload_end,
                                    options.situation_mask));
            options.situation_mask = workload_normal;
        }

        if (options.situation_mask == workload_normal)
        {
            StressWorkload::runNormalWorkload(options);
        }
        else
        {
#define checkAndRun(mask, flag, function)   \
    do                                      \
    {                                       \
        if (mask & flag)                    \
        {                                   \
            running_without_timeout = true; \
            function(options);              \
        }                                   \
    } while (0);

            checkAndRun(
                options.situation_mask,
                workload_heavy_memory_cost_in_snapshot,
                StressWorkload::runHeavyCostInLegacyCompactWorkload);
            checkAndRun(options.situation_mask, workload_page_file_update_long_time, StressWorkload::runPageFileUpdateLongTimeWorkload);
#undef checkAndRun
        }
    }

private:
    const UInt64 workload_normal = 0;
    const UInt64 workload_heavy_memory_cost_in_snapshot = 0x1;
    const UInt64 workload_page_file_update_long_time = 0x2;
    // shold be sum(workload 0...workload N) + 1
    const UInt64 workload_end = 0x4;

    StressEnv options;
};


int main(int argc, char ** argv)
try
{
    StressEnv::initGlobalLogger();
    StressEnv env = StressEnv::parse(argc, argv);
    env.setup();

    StressWorkloadManger(env).runWorkload();

    return running_without_exception ? 0 : -1;
}
catch (...)
{
    DB::tryLogCurrentException("");
    exit(-1);
}
