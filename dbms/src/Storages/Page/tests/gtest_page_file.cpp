#include <Poco/Logger.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageFile.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

TEST(PageFile_test, Compare)
{
    // clean up
    const String path = TiFlashTestEnv::getTemporaryPath() + "/page_file_test";
    {
        if (Poco::File p(path); p.exists())
        {
            Poco::File file(Poco::Path(path).parent());
            file.remove(true);
        }
    }

    const auto     file_provider = TiFlashTestEnv::getContext().getFileProvider();
    Poco::Logger * log           = &Poco::Logger::get("PageFile");

    {
        // Create files for tests
        PageFile checkpoint_pf = PageFile::newPageFile(55, 0, path, file_provider, PageFile::Type::Temp, log);
        auto     writer        = checkpoint_pf.createWriter(false, true);
        checkpoint_pf.setCheckpoint();
        PageFile pf0 = PageFile::newPageFile(2, 0, path, file_provider, PageFile::Type::Formal, log);
        writer       = pf0.createWriter(false, true);
        PageFile pf1 = PageFile::newPageFile(55, 1, path, file_provider, PageFile::Type::Formal, log);
        writer       = pf1.createWriter(false, true);
    }

    PageFile checkpoint_pf = PageFile::openPageFileForRead(55, 0, path, file_provider, PageFile::Type::Checkpoint, log);
    PageFile pf0           = PageFile::openPageFileForRead(2, 0, path, file_provider, PageFile::Type::Formal, log);
    PageFile pf1           = PageFile::openPageFileForRead(55, 1, path, file_provider, PageFile::Type::Formal, log);

    PageFile::Comparator comp;
    ASSERT_EQ(comp(pf0, pf1), true);
    ASSERT_EQ(comp(pf1, pf0), false);

    // Checkpoin file is less than formal file
    ASSERT_EQ(comp(checkpoint_pf, pf0), true);
    ASSERT_EQ(comp(pf0, checkpoint_pf), false);

    // Test compare in `PageFileSet`
    PageFileSet pf_set;
    pf_set.emplace(pf0);
    pf_set.emplace(pf1);
    pf_set.emplace(checkpoint_pf);

    ASSERT_EQ(pf_set.begin()->getType(), PageFile::Type::Checkpoint);
    ASSERT_EQ(pf_set.begin()->fileIdLevel(), checkpoint_pf.fileIdLevel());
    ASSERT_TRUE(pf_set.begin()->isExist());
    ASSERT_EQ(pf_set.rbegin()->getType(), PageFile::Type::Formal);
    ASSERT_EQ(pf_set.rbegin()->fileIdLevel(), pf1.fileIdLevel());
    ASSERT_TRUE(pf_set.rbegin()->isExist());

    // Test `isPageFileExist`
    ASSERT_TRUE(PageFile::isPageFileExist(checkpoint_pf.fileIdLevel(), path, file_provider, PageFile::Type::Checkpoint, log));
    ASSERT_TRUE(PageFile::isPageFileExist(pf0.fileIdLevel(), path, file_provider, PageFile::Type::Formal, log));
    ASSERT_TRUE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Formal, log));
    ASSERT_FALSE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Legacy, log));
    // set pf1 to legacy and check exist
    pf1.setLegacy();
    ASSERT_FALSE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Formal, log));
    ASSERT_TRUE(PageFile::isPageFileExist(pf1.fileIdLevel(), path, file_provider, PageFile::Type::Legacy, log));
}

TEST(PageFile_test, WriteRead)
{
    Poco::Logger *       log           = &Poco::Logger::get("PageFileLink");
    const PageFileId     page_file_id  = 55;
    const PageId         page_id       = 1;
    const UInt64         tag           = 100;
    const size_t         buf_sz        = 1024;
    const PageFieldSizes field_offsets = {10, 20, 30, 50, 914};

    const String path = TiFlashTestEnv::getTemporaryPath("PageFileLink/");
    {
        if (Poco::File p(path); p.exists())
        {
            Poco::File file(Poco::Path(path).parent());
            file.remove(true);
        }
    }

    const auto file_provider = TiFlashTestEnv::getGlobalContext().getFileProvider();
    PageFile   pf0           = PageFile::newPageFile(page_file_id, 0, path, file_provider, PageFile::Type::Formal, log);

    WriteBatch batch;
    {
        char c_buff1[buf_sz], c_buff2[buf_sz], c_buff3[buf_sz], c_buff4[buf_sz], c_buff5[buf_sz];

        for (size_t i = 0; i < buf_sz; ++i)
        {
            c_buff1[i] = i & 0xff;
        }

        memcpy(c_buff2, c_buff1, buf_sz);
        memcpy(c_buff3, c_buff1, buf_sz);
        memcpy(c_buff4, c_buff1, buf_sz);
        memcpy(c_buff5, c_buff1, buf_sz);

        ReadBufferPtr buff1 = std::make_shared<ReadBufferFromMemory>(c_buff1, sizeof(c_buff1));
        ReadBufferPtr buff2 = std::make_shared<ReadBufferFromMemory>(c_buff2, sizeof(c_buff2));
        ReadBufferPtr buff3 = std::make_shared<ReadBufferFromMemory>(c_buff3, sizeof(c_buff3));
        ReadBufferPtr buff4 = std::make_shared<ReadBufferFromMemory>(c_buff4, sizeof(c_buff4));
        ReadBufferPtr buff5 = std::make_shared<ReadBufferFromMemory>(c_buff4, sizeof(c_buff4));

        batch.putPage(page_id, tag, buff1, buf_sz);
        batch.putPage(page_id + 1, tag, buff2, buf_sz);
        batch.putPage(page_id + 2, tag, buff3, buf_sz);
        batch.putPage(page_id + 3, tag, buff4, buf_sz);

        batch.delPage(page_id + 2);
        batch.upsertPage(page_id + 3, tag, {}, 0, buf_sz + 1, 0x123, {});
        batch.putRefPage(page_id + 4, page_id);
        batch.putPage(page_id + 5, tag, buff5, buf_sz, field_offsets);
    }

    auto writer = pf0.createWriter(true, true);
    {
        PageEntriesEdit edit;
        auto            bytes = writer->write(batch, edit);
        ASSERT_GT(bytes, 0);
    }

    auto reader = PageFile::MetaMergingReader::createFrom(pf0, nullptr);
    {
        while (reader->hasNext())
        {
            reader->moveNext();
        }

        auto records = reader->getEdits().getRecords();
        {
            auto record = records.begin();
            // Check write batch 1 - 4 put
            auto i = 0;
            while (i < 4)
            {
                ASSERT_EQ(record->page_id, page_id + i);
                ASSERT_EQ(record->type, WriteBatch::WriteType::PUT);
                ASSERT_EQ(record->entry.file_id, page_file_id);
                ASSERT_EQ(record->entry.size, buf_sz);
                ASSERT_EQ(record->entry.level, 0);
                ASSERT_EQ(record->entry.tag, tag);
                ASSERT_EQ(record->entry.ref, 1);
                record++;
                i++;
            }

            // Check write batch 5 del
            {
                ASSERT_EQ(record->page_id, page_id + 2);
                ASSERT_EQ(record->type, WriteBatch::WriteType::DEL);
                record++;
            }


            // Check write bach 6 upsert
            {
                ASSERT_EQ(record->page_id, page_id + 3);
                ASSERT_EQ(record->type, WriteBatch::WriteType::UPSERT);
                ASSERT_EQ(record->entry.size, buf_sz + 1);
                ASSERT_EQ(record->entry.level, 0);
                ASSERT_EQ(record->entry.tag, tag);
                ASSERT_EQ(record->entry.ref, 1);
                record++;
            }


            // Check write bach 7 ref
            {

                ASSERT_EQ(record->page_id, page_id + 4);
                ASSERT_EQ(record->ori_page_id, page_id);
                ASSERT_EQ(record->type, WriteBatch::WriteType::REF);
                record++;
            }

            // Check write bach 8 put with field offsets
            {
                ASSERT_EQ(record->page_id, page_id + 5);
                ASSERT_EQ(record->type, WriteBatch::WriteType::PUT);
                ASSERT_EQ(record->entry.file_id, page_file_id);
                ASSERT_EQ(record->entry.size, buf_sz);

                auto fo = record->entry.field_offsets;
                ASSERT_EQ(fo.size(), field_offsets.size());

                UInt64 offset     = -1;
                UInt64 offset_crc = 0;

                // Check field offsets
                for (UInt32 j = 0; j < fo.size(); j++)
                {
                    std::tie(offset, offset_crc) = fo[j];
                    if (j == 0)
                    {
                        ASSERT_EQ(offset, 0);
                    }
                    else
                    {
                        auto offset_exp = 0;
                        for (UInt32 k = 0; k < j; k++)
                            offset_exp += field_offsets[k];
                        ASSERT_EQ(offset, offset_exp);
                    }
                    ASSERT_NE(offset_crc, 0);
                }

                record++;
            }

            ASSERT_EQ(record, records.end());
        }
    }
}

TEST(Page_test, GetField)
{
    const size_t buf_sz = 1024;
    char         c_buff[buf_sz];
    for (size_t i = 0; i < buf_sz; ++i)
        c_buff[i] = i % 0xff;

    Page page;
    page.data = ByteBuffer(c_buff, c_buff + buf_sz);
    std::set<Page::FieldOffset> fields{// {field_index, data_offset}
                                       {2, 0},
                                       {3, 20},
                                       {9, 99},
                                       {10086, 1000}};
    page.field_offsets = fields;

    ASSERT_EQ(page.data.size(), buf_sz);
    auto data = page.getFieldData(2);
    ASSERT_EQ(data.size(), fields.find(3)->offset - fields.find(2)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(2)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 2, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldData(3);
    ASSERT_EQ(data.size(), fields.find(9)->offset - fields.find(3)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(3)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 3, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldData(9);
    ASSERT_EQ(data.size(), fields.find(10086)->offset - fields.find(9)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(9)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 9, offset: " << i                                       //
            << ", offset inside page: " << field_offset + i;
    }

    data = page.getFieldData(10086);
    ASSERT_EQ(data.size(), buf_sz - fields.find(10086)->offset);
    for (size_t i = 0; i < data.size(); ++i)
    {
        auto field_offset = fields.find(10086)->offset;
        EXPECT_EQ(*(data.begin() + i), static_cast<char>((field_offset + i) % 0xff)) //
            << "field index: 10086, offset: " << i                                   //
            << ", offset inside page: " << field_offset + i;
    }

    ASSERT_THROW({ page.getFieldData(0); }, DB::Exception);
}

TEST(PageEntry_test, GetFieldInfo)
{
    PageEntry                entry;
    PageFieldOffsetChecksums field_offsets{{0, 0}, {20, 0}, {64, 0}, {99, 0}, {1024, 0}};
    entry.size          = 40000;
    entry.field_offsets = field_offsets;

    size_t beg, end;
    std::tie(beg, end) = entry.getFieldOffsets(0);
    ASSERT_EQ(beg, 0UL);
    ASSERT_EQ(end, 20UL);
    ASSERT_EQ(entry.getFieldSize(0), 20UL - 0);

    std::tie(beg, end) = entry.getFieldOffsets(1);
    ASSERT_EQ(beg, 20UL);
    ASSERT_EQ(end, 64UL);
    ASSERT_EQ(entry.getFieldSize(1), 64UL - 20);

    std::tie(beg, end) = entry.getFieldOffsets(2);
    ASSERT_EQ(beg, 64UL);
    ASSERT_EQ(end, 99UL);
    ASSERT_EQ(entry.getFieldSize(2), 99UL - 64);

    std::tie(beg, end) = entry.getFieldOffsets(3);
    ASSERT_EQ(beg, 99UL);
    ASSERT_EQ(end, 1024UL);
    ASSERT_EQ(entry.getFieldSize(3), 1024UL - 99);

    std::tie(beg, end) = entry.getFieldOffsets(4);
    ASSERT_EQ(beg, 1024UL);
    ASSERT_EQ(end, entry.size);
    ASSERT_EQ(entry.getFieldSize(4), entry.size - 1024);

    ASSERT_THROW({ entry.getFieldOffsets(5); }, DB::Exception);
    ASSERT_THROW({ entry.getFieldSize(5); }, DB::Exception);
}

} // namespace tests
} // namespace DB
