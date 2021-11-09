#include <Encryption/MockKeyManager.h>
#include <IO/ReadBufferFromMemory.h>
#include <TestUtils/MockDiskDelegator.h>

#include <thread>

#include "../NPageFile.h"
#include "../pagemap/PageMap.h"

namespace DB
{
// Define is_background_thread for this binary
// It is required for `RateLimiter` but we do not link with `BackgroundProcessingPool`.
#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif
} // namespace DB
using namespace DB;


void test_bitmap()
{
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);
    String path = "./new-page-storage-test/";
    auto pagemap = NPageMap::newPageMap(path, 2, file_provider);

    UInt64 range1[8] = {
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8};

    UInt64 range2[8];
    pagemap->getDataRange(range1, 8, range2, true);
    // todo
}

void test_wr()
{
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);
    DB::NPageFile pf("./new-page-storage-test/", file_provider);

    const PageId page_id = 1;
    const UInt64 tag = 100;
    const size_t buf_sz = 1024;
    const PageFieldSizes field_offsets = {10, 20, 30, 50, 914};

    WriteBatch batch;
    char c_buff[buf_sz];

    for (size_t i = 0; i < buf_sz; ++i)
    {
        c_buff[i] = i & 0xff;
    }

    ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
    ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
    ReadBufferPtr buff3 = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
    ReadBufferPtr buff4 = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
    ReadBufferPtr buff5 = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));
    c_buff[1022] = 'a';
    ReadBufferPtr buff6 = std::make_shared<ReadBufferFromMemory>(c_buff, sizeof(c_buff));

    batch.putPage(page_id, tag, buff1, buf_sz);
    batch.putPage(page_id + 1, tag, buff2, buf_sz);
    batch.putPage(page_id + 2, tag, buff3, buf_sz);
    batch.putPage(page_id + 3, tag, buff4, buf_sz);
    batch.putPage(page_id + 5, tag, buff5, buf_sz, field_offsets);

    batch.delPage(page_id + 2);
    batch.upsertPage(page_id + 3, tag, {}, buff6, buf_sz, {});
    batch.putRefPage(page_id + 4, page_id);

    pf.write(std::move(batch));

    DB::PageHandler handler = [buff6](DB::PageId page_id, const DB::Page & page) {
        (void)page_id;
        if (strncmp(buff6->buffer().begin(), page.data.begin(), page.data.size()) != 0)
        {
            abort();
        }
    };

    pf.read(page_id + 3, handler);
}

DB::ReadBufferPtr genBufferFromFix(char * memory, size_t max_size)
{
    size_t gen_size = random() % max_size;
    return std::make_shared<DB::ReadBufferFromMemory>(memory + gen_size, max_size - gen_size);
}

void bench_thread_write(DB::NPageFile * page_file)
{
    size_t memory_size = DB::MB * 2;
    char * memory = static_cast<char *>(malloc(memory_size));
    while (true)
    {
        WriteBatch batch;
        for (int i = 0; i < 4; i++)
        {
            auto buffer = genBufferFromFix(memory, DB::MB * 2);
            // ReadBufferPtr buffer = std::make_shared<ReadBufferFromMemory>(buffer, buffer->buffer().size());
            batch.putPage(random() % 10000, 0, buffer, buffer->buffer().size());
        }

        page_file->write(std::move(batch));
    }
}

void test_bench()
{
    srand(time(nullptr));
    size_t thread_nums = 4;
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(std::make_shared<DB::MockKeyManager>(false), false);
    DB::NPageFile * pf = new DB::NPageFile("./new-page-storage-test/", file_provider);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < thread_nums; i++)
    {
        std::thread t(bench_thread_write, pf);
        threads.emplace_back(std::move(t));
    }

    threads[0].join();
    delete pf;
}


int main(int argc, char ** argv)
{
    (void)argc;
    (void)argv;
    // test_bitmap();
    // test_wr();
    test_bench();

    return 0;
}