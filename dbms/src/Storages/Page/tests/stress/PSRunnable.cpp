#include <Common/MemoryTracker.h>
#include <Common/formatReadable.h>
#include <Encryption/MockKeyManager.h>
#include <IO/ReadBufferFromMemory.h>
#include <PSRunnable.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <TestUtils/MockDiskDelegator.h>
#include <fmt/format.h>

#include <random>

void PSRunnable::run()
{
    MemoryTracker tracker;
    tracker.setDescription(nullptr);
    current_memory_tracker = &tracker;
    // If runImpl() return false, means it need break itself
    while (StressEnvStatus::getInstance().isRunning() && runImpl())
    {
        /*Just for no warning*/
    }
    auto peak = current_memory_tracker->getPeak();
    current_memory_tracker = nullptr;
    LOG_INFO(StressEnv::logger, description() << " exit with peak memory usage: " << formatReadableSizeWithBinarySuffix(peak));
}

size_t PSRunnable::getBytesUsed() const
{
    return bytes_used;
}

size_t PSRunnable::getPagesUsed() const
{
    return pages_used;
}

size_t PSWriter::approx_page_mb = 2;
void PSWriter::setApproxPageSize(size_t size_mb)
{
    LOG_INFO(StressEnv::logger, fmt::format("Page approx size is set to {} MB", size_mb));
    approx_page_mb = size_mb;
}

DB::ReadBufferPtr PSWriter::genRandomData(const DB::PageId pageId, DB::MemHolder & holder)
{
    // fill page with random bytes
    const size_t buff_sz = approx_page_mb * DB::MB + random() % 3000;
    char * buff = static_cast<char *>(malloc(buff_sz));
    const char buff_ch = pageId % 0xFF;
    memset(buff, buff_ch, buff_sz);

    holder = DB::createMemHolder(buff, [&](char * p) { free(p); });

    return std::make_shared<DB::ReadBufferFromMemory>(buff, buff_sz);
}

void PSWriter::fillAllPages(const PSPtr & ps)
{
    for (DB::PageId page_id = 0; page_id < MAX_PAGE_ID_DEFAULT; ++page_id)
    {
        DB::MemHolder holder;
        DB::ReadBufferPtr buff = genRandomData(page_id, holder);

        DB::WriteBatch wb;
        wb.putPage(page_id, 0, buff, buff->buffer().size());
        ps->write(std::move(wb));
        if (page_id % 100 == 0)
            LOG_INFO(StressEnv::logger, fmt::format("writer wrote page {}", page_id));
    }
}

bool PSWriter::runImpl()
{
    assert(ps != nullptr);
    const DB::PageId page_id = genRandomPageId();

    DB::MemHolder holder;
    DB::ReadBufferPtr buff = genRandomData(page_id, holder);

    DB::WriteBatch wb;
    wb.putPage(page_id, 0, buff, buff->buffer().size());
    ps->write(std::move(wb));
    ++pages_used;
    bytes_used += buff->buffer().size();
    return true;
}

DB::PageId PSWriter::genRandomPageId()
{
    std::normal_distribution<> distribution{static_cast<double>(max_page_id) / 2, 150};
    return static_cast<DB::PageId>(std::round(distribution(gen))) % max_page_id;
}

void PSCommonWriter::updatedRandomData()
{
    buff_ptrs.clear();
    batch_buffer_size = genBufferSize();
    for (size_t i = 0; i < batch_buffer_nums; ++i)
    {
        char * buff = static_cast<char *>(malloc(batch_buffer_size));
        DB::MemHolder holder = DB::createMemHolder(buff, [&](char * p) { free(p); });
        buff_ptrs.push_back(std::make_shared<DB::ReadBufferFromMemory>(buff, batch_buffer_size));
    }
}

bool PSCommonWriter::runImpl()
{
    assert(ps != nullptr);
    const DB::PageId page_id = genRandomPageId();

    DB::WriteBatch wb;
    updatedRandomData();

    for (auto & buffptr : buff_ptrs)
    {
        wb.putPage(page_id, 0, buffptr, batch_buffer_size);
        ++pages_used;
        bytes_used += batch_buffer_size;
    }

    ps->write(std::move(wb));
    return (batch_buffer_limit == 0 || bytes_used < batch_buffer_limit);
}

void PSCommonWriter::setBatchBufferNums(size_t numbers)
{
    batch_buffer_nums = numbers;
}

void PSCommonWriter::setBatchBufferSize(size_t size)
{
    batch_buffer_size = size;
}

void PSCommonWriter::setBatchBufferLimit(size_t size_limit)
{
    batch_buffer_limit = size_limit;
}

void PSCommonWriter::setBatchBufferPageRange(size_t max_page_id_)
{
    max_page_id = max_page_id_;
}

DB::PageId PSCommonWriter::genRandomPageId()
{
    std::uniform_int_distribution<> dist(0, max_page_id);
    return static_cast<DB::PageId>(dist(gen));
}

void PSCommonWriter::setBatchBufferRange(size_t min, size_t max)
{
    if (max > min && min > 0)
    {
        buffer_size_min = min;
        buffer_size_max = max;
    }
}

size_t PSCommonWriter::genBufferSize()
{
    // If set min/max size set, use the range. Otherwise, use batch_buffer_size.
    if (buffer_size_min <= buffer_size_max && buffer_size_max > 0)
    {
        std::uniform_int_distribution<> dist(buffer_size_min, buffer_size_max);
        return dist(gen);
    }
    return batch_buffer_size;
}

bool PSReader::runImpl()
{
    assert(ps != nullptr);

    std::vector<DB::PageId> page_ids;
    for (size_t i = 0; i < page_read_once; ++i)
    {
        page_ids.emplace_back(random() % max_page_id);
    }

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
    ps->read(page_ids, handler);
    return true;
}

void PSReader::setReadDelay(size_t delay_ms)
{
    heavy_read_delay_ms = delay_ms;
}

void PSReader::setReadPageRange(size_t max_page_id_)
{
    max_page_id = max_page_id_;
}

void PSReader::setReadPageNums(size_t page_read_once_)
{
    page_read_once = page_read_once_;
}
