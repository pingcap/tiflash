#include <Common/MemoryTracker.h>
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
    current_memory_tracker = &tracker;
    // If runImpl() return false, means it need break itself
    while (StressEnvStatus::getInstance().stat() && runImpl())
    {
        /*Just for no warming*/
    }
    const auto desc = description();
    const size_t desc_size = strlen(desc.c_str());
    char desc_buff[desc_size];
    strncpy(desc_buff, desc.c_str(), desc_size);
    tracker.setDescription(desc_buff);

    current_memory_tracker = nullptr;
    LOG_INFO(StressEnv::logger, desc + " exit");
}

size_t PSRunnable::getBytesUsed()
{
    return bytes_used;
}

size_t PSRunnable::getPagesUsed()
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
    char * buff = (char *)malloc(buff_sz);
    const char buff_ch = pageId % 0xFF;
    memset(buff, buff_ch, buff_sz);

    holder = DB::createMemHolder(buff, [&](char * p) { free(p); });

    return std::make_shared<DB::ReadBufferFromMemory>(buff, buff_sz);
}

void PSWriter::fillAllPages(const PSPtr & ps)
{
    for (DB::PageId pageId = 0; pageId < MAX_PAGE_ID_DEFAULT; ++pageId)
    {
        DB::MemHolder holder;
        DB::ReadBufferPtr buff = genRandomData(pageId, holder);

        DB::WriteBatch wb;
        wb.putPage(pageId, 0, buff, buff->buffer().size());
        ps->write(std::move(wb));
        if (pageId % 100 == 0)
            LOG_INFO(StressEnv::logger, fmt::format("writer wrote page {}", pageId));
    }
}

bool PSWriter::runImpl()
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

DB::PageId PSWriter::genRandomPageId()
{
    std::normal_distribution<> distribution{(double)max_page_id / 2, 150};
    return static_cast<DB::PageId>(std::round(distribution(gen))) % max_page_id;
}

void PSCommonWriter::updatedRandomData()
{
    buffPtrs.clear();
    batch_buffer_size = genBufferSize();
    for (size_t i = 0; i < batch_buffer_nums; ++i)
    {
        char * buff = (char *)malloc(batch_buffer_size);
        DB::MemHolder holder = DB::createMemHolder(buff, [&](char * p) { free(p); });
        buffPtrs.push_back(std::make_shared<DB::ReadBufferFromMemory>(buff, batch_buffer_size));
    }
}

bool PSCommonWriter::runImpl()
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
    return static_cast<DB::PageId>(rand() % max_page_id);
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
    // If set min/max size set, use the range. Otherwise , use batch_buffer_size.
    if (buffer_size_min <= buffer_size_max && buffer_size_max > 0)
    {
        return (rand() % (buffer_size_max - buffer_size_min) + buffer_size_min);
    }
    return batch_buffer_size;
}

bool PSReader::runImpl()
{
    assert(ps != nullptr);

    std::vector<DB::PageId> pageIds;
    for (size_t i = 0; i < page_read_once; ++i)
    {
        pageIds.emplace_back(random() % max_page_id);
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
    ps->read(pageIds, handler);
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
