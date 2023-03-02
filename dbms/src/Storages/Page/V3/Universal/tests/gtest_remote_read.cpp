// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Storages/Page/V3/CheckpointFile/CPDataFileReader.h>
#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>

namespace DB
{
namespace PS::universal::tests
{
class UniPageStorageRemoteReadTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        remote_dir = path;
        createIfNotExist(path);
        file_provider = DB::tests::TiFlashTestEnv::getGlobalContext().getFileProvider();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create("test.t", delegator, config, file_provider, remote_dir);
        page_storage->restore();

        log = Logger::get("UniPageStorageRemoteReadTest");
    }

    void reload()
    {
        page_storage = reopenWithConfig(config);
    }

    std::shared_ptr<UniversalPageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = UniversalPageStorage::create("test.t", delegator, config_, file_provider, remote_dir);
        storage->restore();
        return storage;
    }

protected:
    String remote_dir;
    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    PageStorageConfig config;
    std::shared_ptr<UniversalPageStorage> page_storage;

    LoggerPtr log;
};

TEST_F(UniPageStorageRemoteReadTest, WriteRead)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path = remote_dir + "/data_1",
        .data_file_id = "data_1",
        .manifest_file_path = remote_dir + "/manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyRemoteInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();

    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .file_path = remote_dir + "/manifest_foo",
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(1, r.size());

        UniversalWriteBatch wb;
        wb.putRemotePage(r[0].page_id, 0, r[0].entry.checkpoint_info->data_location, std::move(r[0].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    {
        auto page = page_storage->read("aaabbb");
        ASSERT_TRUE(page.isValid());
        ASSERT_EQ("nahida opened her eyes", String(page.data.begin(), page.data.size()));
    }
}
CATCH

TEST_F(UniPageStorageRemoteReadTest, WriteReadMultiple)
try
{
    auto writer = PS::V3::CPFilesWriter::create({
        .data_file_path = remote_dir + "/data_1",
        .data_file_id = "data_1",
        .manifest_file_path = remote_dir + "/manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = PS::V3::CPWriteDataSourceFixture::create({{5, "Said she just dreamed a dream"},
                                                                 {10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    UniversalPageId page_id1 = "aaabbb";
    UniversalPageId page_id2 = "aaabbb2";
    {
        auto edits = PS::V3::universal::PageEntriesEdit{};
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = page_id1, .entry = {.size = 29, .offset = 5}});
        edits.appendRecord({.type = PS::V3::EditRecordType::VAR_ENTRY, .page_id = page_id2, .entry = {.size = 22, .offset = 10}});
        writer->writeEditsAndApplyRemoteInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();

    auto manifest_reader = PS::V3::CPManifestFileReader::create({
        .file_path = remote_dir + "/manifest_foo",
    });
    manifest_reader->readPrefix();
    PS::V3::CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(2, r.size());

        UniversalWriteBatch wb;
        wb.putRemotePage(r[0].page_id, 0, r[0].entry.checkpoint_info->data_location, std::move(r[0].entry.field_offsets));
        wb.putRemotePage(r[1].page_id, 0, r[1].entry.checkpoint_info->data_location, std::move(r[1].entry.field_offsets));
        page_storage->write(std::move(wb));
    }

    {
        UniversalPageIds page_ids{page_id1, page_id2};
        auto page_map = page_storage->read(page_ids);
        ASSERT_EQ(page_map.size(), 2);
        auto & page1 = page_map.at(page_id1);
        ASSERT_EQ("Said she just dreamed a dream", String(page1.data.begin(), page1.data.size()));
        auto & page2 = page_map.at(page_id2);
        ASSERT_EQ("nahida opened her eyes", String(page2.data.begin(), page2.data.size()));
    }
}
CATCH

} // namespace PS::universal::tests
} // namespace DB
