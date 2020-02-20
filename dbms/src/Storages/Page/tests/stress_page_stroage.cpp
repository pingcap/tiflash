#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>

#include <IO/ReadBufferFromMemory.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <common/logger_useful.h>

#include <IO/ReadBufferFromMemory.h>
#include <Storages/Page/PageStorage.h>
#include <common/logger_useful.h>

using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

using PSPtr = std::shared_ptr<DB::PageStorage>;

const DB::PageId MAX_PAGE_ID = 1000;

std::atomic<bool> running_without_exception = true;
std::atomic<bool> running_without_timeout   = true;

size_t                  num_writer_slots     = 1;
std::atomic<DB::UInt64> write_batch_sequence = 0;

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
        LOG_INFO(&Logger::get("root"), "Page approx size is set to " + DB::toString(size_mb) + "MB");
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
            ps->write(wb);
            if (pageId % 100 == 0)
                LOG_INFO(&Logger::get("root"), "writer wrote page" + DB::toString(pageId));
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
            ps->write(wb);
            ++pages_written;
            bytes_written += buff->buffer().size();
            // LOG_INFO(&Logger::get("root"), "writer[" + DB::toString(index) + "] wrote page" + DB::toString(pageId));
        }
        LOG_INFO(&Logger::get("root"), "writer[" + DB::toString(index) + "] exit");
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
                LOG_TRACE(&Logger::get("root"), e.displayText());
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
                LOG_TRACE(&Logger::get("root"), e.displayText());
            }
#endif
        }
        LOG_INFO(&Logger::get("root"), "reader[" + DB::toString(index) + "] exit");
    }
};

class PSGc
{
    PSPtr ps;

public:
    PSGc(const PSPtr & ps_) : ps(ps_) {}
    void onTime(Poco::Timer & /* t */)
    {
        assert(ps != nullptr);
        try
        {
            ps->gc();
        }
        catch (DB::Exception & e)
        {
            // if gc throw exception stop the test
            running_without_exception = false;
        }
    }
};

class StressTimeout
{
public:
    void onTime(Poco::Timer & /* t */)
    {
        LOG_INFO(&Logger::get("root"), "timeout.");
        running_without_timeout = false;
    }
};

int main(int argc, char ** argv)
{
    (void)argc;
    (void)argv;

    Poco::AutoPtr<Poco::ConsoleChannel>   channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Logger::root().setChannel(formatting_channel);
    Logger::root().setLevel("trace");

    bool   drop_before_run     = false;
    long   timeout_s           = 0;
    size_t num_writers         = 1;
    size_t num_readers         = 4;
    size_t heavy_read_delay_ms = 0;
    num_writer_slots           = 1;
    if (argc >= 2)
    {
        DB::String drop_str = argv[1];
        if (drop_str == "drop")
            drop_before_run = true;
        // timeout for benchmark
        if (argc >= 3)
            timeout_s = strtol(argv[2], nullptr, 10);
        // num writers
        if (argc >= 4)
            num_writers = strtoul(argv[3], nullptr, 10);
        // num readers
        if (argc >= 5)
            num_readers = strtoul(argv[4], nullptr, 10);
        if (argc >= 6)
        {
            // 2 by default.
            size_t page_mb = strtoul(argv[5], nullptr, 10);
            page_mb        = std::max(page_mb, 1UL);
            PSWriter::setApproxPageSize(page_mb);
        }
        if (argc >= 7)
        {
            // 0 by default.
            heavy_read_delay_ms = strtoul(argv[6], nullptr, 10);
            heavy_read_delay_ms = std::max(heavy_read_delay_ms, 0);
            LOG_INFO(&Logger::get("root"), "read dealy: " + DB::toString(heavy_read_delay_ms) + "ms");
        }
        if (argc >= 8)
        {
            // 1 by default.
            num_writer_slots = strtoul(argv[7], nullptr, 10);
            num_writer_slots = std::max(num_writer_slots, 1UL);
        }
    }
    // set random seed
    srand(0x123987);

    const DB::String path = "./stress";
    // drop dir if exists
    Poco::File file(path);
    if (file.exists() && drop_before_run)
    {
        LOG_INFO(&Logger::get("root"), "All pages have been drop.");
        file.remove(true);
    }

    // create PageStorage
    DB::PageStorage::Config config;
    config.num_write_slots = num_writer_slots;
    PSPtr ps               = std::make_shared<DB::PageStorage>("stress_test", path, config);
    ps->restore();
    {
        size_t num_of_pages = 0;
        auto   check        = [&num_of_pages](const DB::Page & page) {
            (void)page;
            num_of_pages++;
        };
        ps->traverse(check);
        LOG_INFO(&Logger::get("root"), "Recover " << num_of_pages << " pages.");
    }

    // init all pages in PageStorage
    PSWriter::fillAllPages(ps);
    LOG_INFO(&Logger::get("root"), "All pages have been init.");

    high_resolution_clock::time_point beginTime = high_resolution_clock::now();

    // create thread pool
    LOG_INFO(&Logger::get("root"),
             "start running with these threads: W:" + DB::toString(num_writers) + ",R:" + DB::toString(num_readers)
                 + ",Gc:1, config.num_writer_slots:" + DB::toString(num_writer_slots));
    Poco::ThreadPool pool(/* minCapacity= */ 1 + num_writers + num_readers, 1 + num_writers + num_readers);

    // start one writer thread
    std::vector<std::shared_ptr<PSWriter>> writers(num_writers);
    for (size_t i = 0; i < num_writers; ++i)
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

    // start multiple read thread
    std::vector<std::shared_ptr<PSReader>> readers(num_readers);
    for (size_t i = 0; i < num_readers; ++i)
    {
        readers[i] = std::make_shared<PSReader>(ps, i, heavy_read_delay_ms);
        pool.start(*readers[i], "reader" + DB::toString(i));
    }

    // set timeout
    Poco::Timer   timeout_timer(timeout_s);
    StressTimeout canceler;
    if (timeout_s > 0)
    {
        LOG_INFO(&Logger::get("root"), "benchmark timeout: " + DB::toString(timeout_s) + "s");
        timeout_timer.setStartInterval(timeout_s * 1000);
        timeout_timer.start(Poco::TimerCallback<StressTimeout>(canceler, &StressTimeout::onTime));
    }

    pool.joinAll();
    high_resolution_clock::time_point endTime      = high_resolution_clock::now();
    milliseconds                      timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
    fprintf(stderr, "end in %ldms\n", timeInterval.count());
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
    fprintf(stderr,
            "W: %zu pages, %.4lf GB, %.4lf GB/s\n",
            total_pages_written,
            total_bytes_written / GB,
            total_bytes_written / GB / seconds_run);
    fprintf(stderr, "R: %zu pages, %.4lf GB, %.4lf GB/s\n", total_pages_read, total_bytes_read / GB, total_bytes_read / GB / seconds_run);

    if (running_without_exception)
    {
        return 0;
    }
    else
    {
        return -1;
    }
}
