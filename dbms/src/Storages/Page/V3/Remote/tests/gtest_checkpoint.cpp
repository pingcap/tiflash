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

#include <IO/ReadBufferFromFile.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/Remote/CheckpointFilesWriter.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Page/V3/Remote/CheckpointPageManager.h>
#include <Storages/Page/universal/Readers.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>

namespace DB
{
namespace PS::universal::tests
{
class CheckpointTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        createIfNotExist(path);
        file_provider = DB::tests::TiFlashTestEnv::getGlobalContext().getFileProvider();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create("test.t", delegator, config, file_provider);
        page_storage->restore();

        for (size_t i = 0; i < buf_sz; ++i)
        {
            c_buff[i] = i % 0xff;
        }

        log = Logger::get("PageStorageTest");
    }

    std::shared_ptr<UniversalPageStorage> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = UniversalPageStorage::create("test.t", delegator, config_, file_provider);
        storage->restore();
        return storage;
    }

protected:
    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    PageStorageConfig config;
    std::shared_ptr<UniversalPageStorage> page_storage;

    LoggerPtr log;

    static constexpr size_t buf_sz = 1024;
    char c_buff[buf_sz] = {};
};

TEST_F(CheckpointTest, CheckpointManagerGetNormalPageId)
{
    UInt64 tag = 0;
    {
        UniversalWriteBatch wb;
        c_buff[0] = 10;
        c_buff[1] = 1;
        wb.putPage(RaftLogReader::toFullPageId(100, 100), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        wb.putRefPage(RaftLogReader::toFullPageId(10, 4), RaftLogReader::toFullPageId(100, 100));
        wb.putRefPage(RaftLogReader::toFullPageId(102, 4), RaftLogReader::toFullPageId(10, 4));
        wb.putRefPage(RaftLogReader::toFullPageId(202, 4), RaftLogReader::toFullPageId(102, 4));
        wb.putRefPage(RaftLogReader::toFullPageId(302, 4), RaftLogReader::toFullPageId(202, 4));

        page_storage->write(std::move(wb));
    }

    using PS::RemoteDataLocation;
    using PS::V3::CheckpointManifestFileReader;
    using PS::V3::CheckpointPageManager;
    using PS::V3::PageDirectory;
    using PS::V3::Remote::WriterInfo;
    using PS::V3::universal::BlobStoreTrait;
    using PS::V3::universal::PageDirectoryTrait;

    // do checkpoint
    auto checkpoint_dir = getTemporaryPath() + "/";
    {
        auto writer_info = std::make_shared<PS::V3::Remote::WriterInfo>();
        page_storage->checkpoint_manager->dumpRemoteCheckpoint(V3::CheckpointUploadManager::DumpRemoteCheckpointOptions{
            .temp_directory = checkpoint_dir + "temp/",
            .writer_info = writer_info,
        });
    }

    // read from checkpoint
    {
        UInt64 latest_manifest_sequence = 0;
        Poco::DirectoryIterator it(checkpoint_dir);
        Poco::DirectoryIterator end;
        while (it != end)
        {
            if (it->isFile())
            {
                const Poco::Path & file_path = it->path();
                if (file_path.getExtension() == "manifest")
                {
                    auto current_manifest_sequence = UInt64(std::stoul(file_path.getBaseName()));
                    latest_manifest_sequence = std::max(current_manifest_sequence, latest_manifest_sequence);
                }
            }
            ++it;
        }
        ASSERT_TRUE(latest_manifest_sequence > 0);
        auto checkpoint_path = checkpoint_dir + fmt::format("{}.manifest", latest_manifest_sequence);
        auto checkpoint_data_dir = Poco::Path(checkpoint_path).parent().toString();
        auto local_ps = PS::V3::CheckpointPageManager::createTempPageStorage(*db_context, checkpoint_path, checkpoint_data_dir);

        ASSERT_EQ(local_ps->getNormalPageId(RaftLogReader::toFullPageId(100, 100)), RaftLogReader::toFullPageId(100, 100));
        ASSERT_EQ(local_ps->getNormalPageId(RaftLogReader::toFullPageId(10, 4)), RaftLogReader::toFullPageId(100, 100));
        ASSERT_EQ(local_ps->getNormalPageId(RaftLogReader::toFullPageId(102, 4)), RaftLogReader::toFullPageId(100, 100));
        ASSERT_EQ(local_ps->getNormalPageId(RaftLogReader::toFullPageId(202, 4)), RaftLogReader::toFullPageId(100, 100));
        ASSERT_EQ(local_ps->getNormalPageId(RaftLogReader::toFullPageId(302, 4)), RaftLogReader::toFullPageId(100, 100));
    }
}

} // namespace PS::universal::tests
} // namespace DB
