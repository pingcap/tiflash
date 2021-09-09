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
    tracker.setDescription(description().c_str());
    current_memory_tracker = nullptr;
    LOG_INFO(StressEnv::logger, description() + " exit");
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

DB::PageId writing_page[1000];

bool PSCommonWriter::runImpl()
{
    const DB::PageId pageId = genRandomPageId();

    DB::WriteBatch wb;
    updatedRandomData();

    for (auto & buffptr : buffPtrs)
    {
        wb.putPage(pageId, 0, buffptr, batch_buffer_size);
        ++pages_used;
        bytes_used += batch_buffer_size;
    }
    writing_page[index] = pageId;
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


DB::PageIds PSReader::genRandomPageIds()
{
    DB::PageIds pageIds;
    for (size_t i = 0; i < page_read_once; ++i)
    {
        pageIds.emplace_back(random() % max_page_id);
    }
    return pageIds;
}

bool PSReader::runImpl()
{
    DB::PageIds pageIds;
    assert(ps != nullptr);

    pageIds = genRandomPageIds();
    if (pageIds.size() == 0)
    {
        return true;
    }

    DB::PageHandler handler = [&](DB::PageId page_id, const DB::Page & page) {
        (void)page_id;
        // use `sleep` to mock heavy read
        if (heavy_read_delay_ms > 0)
        {
            usleep(heavy_read_delay_ms * 1000);
        }
        ++pages_used;
        bytes_used += page.data.size();
    };
    ps->read(pageIds, handler);
    return true;
}

void PSReader::setPageReadOnce(size_t page_read_once_)
{
    page_read_once = page_read_once_;
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

void PSWindowWriter::setWindowSize(size_t window_size_)
{
    window_size = window_size_;
}

void PSWindowWriter::setNormalDistributionSigma(size_t sigma_)
{
    sigma = sigma_;
}

UInt64 pageid_boundary = 0;
std::mutex _page_id_mutex;

DB::PageId PSWindowWriter::genRandomPageId()
{
    std::lock_guard<std::mutex> _lock(_page_id_mutex);
    if (pageid_boundary < (window_size / 2))
    {
        return static_cast<DB::PageId>(pageid_boundary++);
    }

    // Generate a random number in the window
    std::normal_distribution<> distribution{(double)window_size, (double)sigma};
    auto random = std::round(distribution(gen));
    // Move this "random" near the pageid_boundary, If "random" is still negative, then make it positive
    random = std::abs(random + pageid_boundary);
    return static_cast<DB::PageId>(random > pageid_boundary ? pageid_boundary++ : random);
}

void PSWindowReader::setWindowSize(size_t window_size_)
{
    window_size = window_size_;
}

void PSWindowReader::setNormalDistributionSigma(size_t sigma_)
{
    sigma = sigma_;
}

void PSWindowReader::setWriterNums(size_t writer_nums_)
{
    writer_nums = writer_nums_;
}

DB::PageIds PSWindowReader::genRandomPageIds()
{
    std::vector<DB::PageId> pageIds;

    if (pageid_boundary <= (writer_nums + page_read_once))
    {
        // Nothing to read
        return pageIds;
    }

    size_t read_boundary = pageid_boundary - writer_nums - page_read_once;
    if (read_boundary < window_size)
    {
        return pageIds;
    }

    std::normal_distribution<> distribution{(double)window_size, (double)sigma};
    auto random = std::round(distribution(gen));

    random = read_boundary - window_size + random;

    // Bigger than window right boundary
    if (random > read_boundary)
    {
        random = read_boundary;
    }

    // Smaller than window left boundary
    if (random < 0)
    {
        random = std::abs(random);
    }

    for (size_t i = random; i < page_read_once + random; ++i)
    {
        bool writing = false;
        for (size_t j = 0; j < writer_nums; j++)
        {
            if (i == writing_page[j])
            {
                writing = true;
            }
        }
        if (!writing)
            pageIds.emplace_back(i);
    }

    return pageIds;
}