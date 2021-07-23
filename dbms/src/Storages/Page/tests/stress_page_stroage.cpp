#include <Common/FailPoint.h>
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
#include <Storages/Page/PageStorage.h>
#include <TestUtils/MockDiskDelegator.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <atomic>
#include <boost/program_options.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>

using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

using PSPtr = std::shared_ptr<DB::PageStorage>;

const DB::PageId MAX_PAGE_ID = 1000;

std::atomic<bool> running_without_exception = true;
std::atomic<bool> running_without_timeout   = true;

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

struct StressOptions
{
    size_t num_writers      = 1;
    size_t num_readers      = 4;
    bool   clean_before_run = false;
    size_t timeout_s        = 0;
    size_t read_delay_ms    = 0;
    size_t num_writer_slots = 1;
    size_t avg_page_size_mb = 1;
    size_t rand_seed        = 0x123987;

    std::vector<std::string> paths;

    std::vector<std::string> failpoints;

    std::string toDebugString() const
    {
        return fmt::format("{{ num_writers: {}, num_readers: {}, clean_before_run: {}" //
                           ", timeout_s: {}, read_delay_ms: {}, num_writer_slots: {}"
                           ", avg_page_size_mb: {}, rand_seed: {:08x} paths: [{}] failpoints: [{}] }}",
                           num_writers,
                           num_readers,
                           clean_before_run,
                           timeout_s,
                           read_delay_ms,
                           num_writer_slots,
                           avg_page_size_mb,
                           rand_seed,
                           fmt::join(paths.begin(), paths.end(), ","),
                           fmt::join(failpoints.begin(), failpoints.end(), ",")
                           //
        );
    }

    // <prog> -W 4 -R 128 -T 10 -C 1 --paths ./stress1 --paths ./stress2
    static StressOptions parse(int argc, char ** argv)
    {
        namespace po = boost::program_options;
        using po::value;
        po::options_description desc("Allowed options");
        desc.add_options()("help,h", "produce help message")                                          //
            ("write_concurrency,W", value<UInt32>()->default_value(4), "number of write threads")     //
            ("read_concurrency,R", value<UInt32>()->default_value(16), "number of read threads")      //
            ("clean_before_run,C", value<bool>()->default_value(false), "drop data before running")   //
            ("timeout,T", value<UInt32>()->default_value(600), "maximum run time (seconds)")          //
            ("writer_slots", value<UInt32>()->default_value(4), "number of PageStorage writer slots") //
            ("read_delay_ms", value<UInt32>()->default_value(0), "millionseconds of read delay")      //
            ("avg_page_size", value<UInt32>()->default_value(1), "avg size for each page(MiB)")       //
            ("rand_seed", value<UInt32>()->default_value(0x123987), "random seed")                    //
            ("paths,P", value<std::vector<std::string>>(), "store path(s)")                           //
            ("failpoints,F", value<std::vector<std::string>>(), "failpoint(s) to enable")             //
            ;
        po::variables_map options;
        po::store(po::parse_command_line(argc, argv, desc), options);
        po::notify(options);
        if (options.count("help") > 0)
        {
            std::cerr << desc << std::endl;
            exit(0);
        }

        StressOptions opt;
        opt.num_writers      = options["write_concurrency"].as<UInt32>();
        opt.num_readers      = options["read_concurrency"].as<UInt32>();
        opt.clean_before_run = options["clean_before_run"].as<bool>();
        opt.timeout_s        = options["timeout"].as<UInt32>();
        opt.read_delay_ms    = options["read_delay_ms"].as<UInt32>();
        opt.num_writer_slots = options["writer_slots"].as<UInt32>();
        opt.avg_page_size_mb = options["avg_page_size"].as<UInt32>();
        opt.rand_seed        = options["rand_seed"].as<UInt32>();
        if (options.count("paths"))
            opt.paths = options["paths"].as<std::vector<std::string>>();
        else
            opt.paths = {"./stress"};
        if (options.count("failpoints"))
            opt.failpoints = options["failpoints"].as<std::vector<std::string>>();
        return opt;
    }
};

class PSWriter : public Poco::Runnable
{
    DB::UInt32   index = 0;
    PSPtr        ps;
    std::mt19937 gen;

    static size_t approx_page_mb;

public:
    PSWriter(const PSPtr & ps_, DB::UInt32 idx) : index(idx), ps(ps_), gen(), bytes_written(0), pages_written(0) {}

    static void setApproxPageSize(size_t size_mb)
    {
        LOG_INFO(logger, fmt::format("Page approx size is set to {} MB", size_mb));
        approx_page_mb = size_mb;
    }

    static DB::ReadBufferPtr genRandomData(const DB::PageId pageId, DB::MemHolder & holder)
    {
        // fill page with random bytes
        const size_t buff_sz = approx_page_mb * 1024 * 1024 + random() % 3000;
        char *       buff    = (char *)malloc(buff_sz);
        const char   buff_ch = pageId % 0xFF;
        memset(buff, buff_ch, buff_sz);

        holder = DB::createMemHolder(buff, [&](char * p) { free(p); });

        return std::make_shared<DB::ReadBufferFromMemory>(buff, buff_sz);
    }

    static void fillAllPages(const PSPtr & ps)
    {
        for (DB::PageId pageId = 0; pageId < MAX_PAGE_ID; ++pageId)
        {
            DB::MemHolder     holder;
            DB::ReadBufferPtr buff = genRandomData(pageId, holder);

            DB::WriteBatch wb;
            wb.putPage(pageId, 0, buff, buff->buffer().size());
            ps->write(std::move(wb));
            if (pageId % 100 == 0)
                LOG_INFO(logger, fmt::format("writer wrote page {}", pageId));
        }
    }

    size_t bytes_written;
    size_t pages_written;

    void run() override
    {
        while (running_without_exception && running_without_timeout)
        {
            assert(ps != nullptr);
            std::normal_distribution<> d{MAX_PAGE_ID / 2, 150};
            const DB::PageId           pageId = static_cast<DB::PageId>(std::round(d(gen))) % MAX_PAGE_ID;
            //const DB::PageId pageId = random() % MAX_PAGE_ID;

            DB::MemHolder     holder;
            DB::ReadBufferPtr buff = genRandomData(pageId, holder);

            DB::WriteBatch wb;
            wb.putPage(pageId, 0, buff, buff->buffer().size());
            ps->write(std::move(wb));
            ++pages_written;
            bytes_written += buff->buffer().size();
            // LOG_INFO(logger, "writer[" + DB::toString(index) + "] wrote page" + DB::toString(pageId));
        }
        LOG_INFO(logger, fmt::format("writer[{}] exit", index));
    }
};

size_t PSWriter::approx_page_mb = 2;

class PSReader : public Poco::Runnable
{
    DB::UInt32   index = 0;
    PSPtr        ps;
    const size_t heavy_read_delay_ms;

public:
    PSReader(const PSPtr & ps_, DB::UInt32 idx, size_t delay_ms)
        : index(idx), ps(ps_), heavy_read_delay_ms(delay_ms), pages_read(0), bytes_read(0)
    {
    }

    size_t pages_read;
    size_t bytes_read;

    void run() override
    {
        while (running_without_exception && running_without_timeout)
        {
            {
                // sleep [0~10) ms
                const uint32_t micro_seconds_to_sleep = random() % 10;
                usleep(micro_seconds_to_sleep * 1000);
            }
            assert(ps != nullptr);
#if 0
            const DB::PageId pageId = random() % MAX_PAGE_ID;
            try
            {
                DB::Page page = ps->read(pageId);
                ++pages_read;
                bytes_read += page.data.size();
            }
            catch (DB::Exception & e)
            {
                LOG_TRACE(logger, e.displayText());
            }
#else
            std::vector<DB::PageId> pageIds;
            for (size_t i = 0; i < 5; ++i)
            {
                pageIds.emplace_back(random() % MAX_PAGE_ID);
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
                    ++pages_read;
                    bytes_read += page.data.size();
                };
                ps->read(pageIds, handler);
            }
            catch (DB::Exception & e)
            {
                DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            }
#endif
        }
        LOG_INFO(logger, fmt::format("reader[{}] exit", index));
    }
};

class PSGc
{
    PSPtr ps;

public:
    PSGc(const PSPtr & ps_) : ps(ps_) { assert(ps != nullptr); }
    void onTime(Poco::Timer & /* t */)
    {
        try
        {
            ps->gc();
        }
        catch (...)
        {
            // if gc throw exception stop the test
            running_without_exception = false;
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }
};

class PSScanner
{
    PSPtr ps;

public:
    PSScanner(const PSPtr & ps_) : ps(ps_) { assert(ps != nullptr); }
    void onTime(Poco::Timer & /* t*/)
    {
        size_t   num_snapshots           = 0;
        double   oldest_snapshot_seconds = 0.0;
        unsigned oldest_snapshot_thread  = 0;
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
};

class StressTimeout
{
public:
    void onTime(Poco::Timer & /* t */)
    {
        LOG_INFO(logger, "timeout.");
        running_without_timeout = false;
    }
};

int main(int argc, char ** argv)
try
{
    Poco::AutoPtr<Poco::ConsoleChannel>    channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter>  formatter(new DB::UnifiedLogPatternFormatter);
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Logger::root().setChannel(formatting_channel);
    Logger::root().setLevel("trace");
    logger = &Poco::Logger::get("root");

#ifdef FIU_ENABLE
    fiu_init(0);
#endif

    StressOptions options = StressOptions::parse(argc, argv);
    LOG_INFO(logger, fmt::format("Options: {}", options.toDebugString()));

    for (const auto & fp : options.failpoints)
    {
        DB::FailPointHelper::enableFailPoint(fp);
    }

    // set random seed
    srand(options.rand_seed);

    bool need_fill_init_pages = false;
    // drop dir if exists
    bool all_directories_not_exist = true;
    for (const auto & path : options.paths)
    {
        if (Poco::File file(path); file.exists())
        {
            all_directories_not_exist = false;
            if (options.clean_before_run)
            {
                file.remove(true);
            }
        }
    }
    if (options.clean_before_run)
        need_fill_init_pages = true;
    else
        need_fill_init_pages = !all_directories_not_exist;
    LOG_INFO(logger, "All pages have been drop.");

    // create PageStorage
    DB::PageStorage::Config config;
    config.num_write_slots            = options.num_writer_slots;
    DB::KeyManagerPtr   key_manager   = std::make_shared<DB::MockKeyManager>(false);
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(key_manager, false);

    // FIXME: running with `MockDiskDelegatorMulti` is not well-testing
    DB::PSDiskDelegatorPtr delegator;
    if (options.paths.empty())
        throw DB::Exception("Can not run without paths");
    if (options.paths.size() == 1)
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0]);
    else
        delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(options.paths);

    PSPtr ps = std::make_shared<DB::PageStorage>("stress_test", delegator, config, file_provider);
    ps->restore();
    {
        size_t num_of_pages = 0;
        ps->traverse([&num_of_pages](const DB::Page & page) {
            (void)page;
            num_of_pages++;
        });
        LOG_INFO(logger, fmt::format("Recover {} pages.", num_of_pages));
    }

    // init all pages in PageStorage
    if (need_fill_init_pages)
    {
        PSWriter::fillAllPages(ps);
        LOG_INFO(logger, "All pages have been init.");
    }

    high_resolution_clock::time_point beginTime = high_resolution_clock::now();

    // create thread pool
    LOG_INFO(logger,
             fmt::format("start running with these threads: "
                         "W:{},R:{},Gc:1, config.num_writer_slots:{}, timeout:{} seconds",
                         options.num_writers,
                         options.num_readers,
                         options.num_writer_slots,
                         options.timeout_s));
    Poco::ThreadPool pool(/* minCapacity= */ 1 + options.num_writers + options.num_readers, 1 + options.num_writers + options.num_readers);

    // start some writer thread
    PSWriter::setApproxPageSize(options.avg_page_size_mb);
    std::vector<std::shared_ptr<PSWriter>> writers(options.num_writers);
    for (size_t i = 0; i < options.num_writers; ++i)
    {
        writers[i] = std::make_shared<PSWriter>(ps, i);
        pool.start(*writers[i], "writer" + DB::toString(i));
    }

    // start one gc thread
    PSGc        gc(ps);
    Poco::Timer timer(0);
    timer.setStartInterval(1000);
    timer.setPeriodicInterval(30 * 1000);
    timer.start(Poco::TimerCallback<PSGc>(gc, &PSGc::onTime));

    PSScanner   scanner(ps);
    Poco::Timer scanner_timer(0);
    scanner_timer.setStartInterval(1000);
    scanner_timer.setPeriodicInterval(30 * 1000);
    scanner_timer.start(Poco::TimerCallback<PSScanner>(scanner, &PSScanner::onTime));

    // start multiple read thread
    std::vector<std::shared_ptr<PSReader>> readers(options.num_readers);
    for (size_t i = 0; i < options.num_readers; ++i)
    {
        readers[i] = std::make_shared<PSReader>(ps, i, options.read_delay_ms);
        pool.start(*readers[i], "reader" + DB::toString(i));
    }

    // set timeout
    Poco::Timer   timeout_timer(options.timeout_s);
    StressTimeout canceler;
    if (options.timeout_s > 0)
    {
        LOG_INFO(logger, fmt::format("benchmark timeout: {}s", options.timeout_s));
        timeout_timer.setStartInterval(options.timeout_s * 1000);
        timeout_timer.start(Poco::TimerCallback<StressTimeout>(canceler, &StressTimeout::onTime));
    }

    pool.joinAll();
    high_resolution_clock::time_point endTime      = high_resolution_clock::now();
    milliseconds                      timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
    fmt::print(stderr, "end in {}ms\n", timeInterval.count());
    double seconds_run = 1.0 * timeInterval.count() / 1000;

    size_t total_pages_written = 0;
    size_t total_bytes_written = 0;
    for (auto & writer : writers)
    {
        total_pages_written += writer->pages_written;
        total_bytes_written += writer->bytes_written;
    }

    size_t total_pages_read = 0;
    size_t total_bytes_read = 0;
    for (auto & reader : readers)
    {
        total_pages_read += reader->pages_read;
        total_bytes_read += reader->bytes_read;
    }

    const double GB = 1024 * 1024 * 1024;
    fmt::print(stderr,
               "W: {} pages, {:.4f} GB, {:.4f} GB/s\n", //
               total_pages_written,
               total_bytes_written / GB,
               total_bytes_written / GB / seconds_run);
    fmt::print(stderr,
               "R: {} pages, {:.4f} GB, {:.4f} GB/s\n", //
               total_pages_read,
               total_bytes_read / GB,
               total_bytes_read / GB / seconds_run);

    if (running_without_exception)
        return 0;
    else
        return -1;
}
catch (...)
{
    DB::tryLogCurrentException("");
    exit(-1);
}
