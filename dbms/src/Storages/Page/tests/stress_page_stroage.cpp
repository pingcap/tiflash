#include <atomic>
#include <iostream>
#include <memory>
#include <random>

#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>

#include <Storages/Page/PageStorage.h>

using PSPtr = std::shared_ptr<DB::PageStorage>;

const DB::PageId MAX_PAGE_ID = 1000;

std::atomic<bool> running_without_exception = true;

class PSWriter : public Poco::Runnable
{
    PSPtr        ps;
    std::mt19937 gen;

public:
    PSWriter(const PSPtr & ps_) : ps(ps_), gen() {}
    void run() override
    {
        while (running_without_exception)
        {
            assert(ps != nullptr);
            std::normal_distribution<> d{MAX_PAGE_ID / 2, 150};
            const DB::PageId           pageId = static_cast<DB::PageId>(std::round(d(gen))) % MAX_PAGE_ID;
            //const DB::PageId pageId = random() % MAX_PAGE_ID;

            DB::WriteBatch wb;
            // fill page with random bytes
            const size_t buff_sz = 2048 * 1024 + random() % 3000;
            char *       buff    = new char[buff_sz];
            const char   buff_ch = random() % 0xFF;
            memset(buff, buff_ch, buff_sz);
            wb.putPage(pageId, 0, std::make_shared<DB::ReadBufferFromMemory>(buff, buff_sz), buff_sz);
            delete[] buff;

            ps->write(wb);
        }
        LOG_INFO(&Logger::get("root"), "writer exit");
    }
};

class PSReader : public Poco::Runnable
{
    PSPtr ps;

public:
    PSReader(const PSPtr & ps_) : ps(ps_) {}
    void run() override
    {
        while (running_without_exception)
        {
            {
                const uint32_t micro_seconds_to_sleep = random() % 50;
                usleep(micro_seconds_to_sleep * 1000);
            }
            assert(ps != nullptr);
            const DB::PageId pageId = random() % MAX_PAGE_ID;
            try
            {
                ps->read({
                    pageId,
                });
            }
            catch (DB::Exception & e)
            {
                LOG_TRACE(&Logger::get("root"), e.displayText());
            }
        }
        LOG_INFO(&Logger::get("root"), "reader exit");
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
            //throw DB::Exception("fake exception");
            ps->gc();
        }
        catch (DB::Exception & e)
        {
            // if gc throw exception stop the test
            running_without_exception = false;
        }
    }
};

int main(int argc, char ** argv)
{
    (void)argc;
    (void)argv;

    bool drop_before_run = false;
    if (argc > 2)
    {
        DB::String drop_str = argv[2];
        if (drop_str == "drop")
        {
            drop_before_run = true;
        }
    }

    Poco::AutoPtr<Poco::ConsoleChannel>   channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Logger::root().setChannel(formatting_channel);
    Logger::root().setLevel("trace");


    const DB::String path = "./stress";
    // drop dir if exists
    Poco::File file(path);
    if (file.exists() && drop_before_run)
    {
        file.remove(true);
    }

    // create PageStorage
    DB::PageStorage::Config config;
    PSPtr                   ps = std::make_shared<DB::PageStorage>(path, config);

    // create thread pool
    const size_t     num_readers = 4;
    Poco::ThreadPool pool(/* minCapacity= */ 2 + num_readers);

    // start one writer thread
    PSWriter writer(ps);
    pool.start(writer, "writer");

    // start one gc thread
    PSGc        gc(ps);
    Poco::Timer timer(0, 30 * 1000);
    timer.setStartInterval(1000);
    timer.setPeriodicInterval(30 * 1000);
    timer.start(Poco::TimerCallback<PSGc>(gc, &PSGc::onTime));

    // start mutiple read thread
    std::vector<std::shared_ptr<PSReader>> readers(num_readers);
    for (size_t i = 0; i < num_readers; ++i)
    {
        readers[i] = std::make_shared<PSReader>(ps);
        pool.start(*readers[i]);
    }

    pool.joinAll();

    return -1;
}
