#pragma once
#include <Poco/Runnable.h>

#include "PSStressEnv.h"

const DB::PageId MAX_PAGE_ID_DEFAULT = 1000;

class PSRunnable : public Poco::Runnable
{
public:
    void run() override;

    size_t getBytesUsed();
    size_t getPagesUsed();

    virtual String description() = 0;
    virtual bool runImpl() = 0;

public:
    size_t bytes_used = 0;
    size_t pages_used = 0;
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

    virtual String description() override
    {
        return fmt::format("(Stress Test Writer {})", index);
    }

    static void setApproxPageSize(size_t size_mb);

    static DB::ReadBufferPtr genRandomData(const DB::PageId pageId, DB::MemHolder & holder);

    static void fillAllPages(const PSPtr & ps);

    virtual bool runImpl() override;

protected:
    virtual DB::PageId genRandomPageId();

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

    void updatedRandomData();

    String description() override { return fmt::format("(Stress Test Common Writer {})", index); }

    bool runImpl() override;

    void setBatchBufferNums(size_t numbers);

    void setBatchBufferSize(size_t size);

    void setBatchBufferLimit(size_t size_limit);

    void setBatchBufferPageRange(size_t max_page_id_);

protected:
    size_t batch_buffer_nums = 100;
    size_t batch_buffer_size = 1 * DB::MB;
    size_t batch_buffer_limit = 0;

    virtual DB::PageId genRandomPageId() override;

private:
    std::vector<DB::ReadBufferPtr> buffPtrs;
};

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

    String description() override { return fmt::format("(Stress Test PSReader {})", index); }

    bool runImpl() override;

protected:
    PSPtr ps;
    DB::UInt32 index = 0;
    DB::PageId max_page_id = MAX_PAGE_ID_DEFAULT;
};
