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

#include <Storages/Page/UniversalPage.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/MockDiskDelegator.h>

namespace DB
{
namespace PS::universal::tests
{
class UniPageStorageTest : public DB::base::TiFlashStorageTestBasic
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

TEST_F(UniPageStorageTest, RaftLog)
{
    UInt64 tag = 0;
    {
        UniversalWriteBatch wb;
        c_buff[0] = 10;
        c_buff[1] = 1;
        wb.putPage(RaftLogReader::toFullPageId(10, 1), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 4;
        wb.putPage(RaftLogReader::toFullPageId(10, 4), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 5;
        wb.putPage(RaftLogReader::toFullPageId(10, 5), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 6;
        wb.putPage(RaftLogReader::toFullPageId(10, 6), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 7;
        wb.putPage(RaftLogReader::toFullPageId(10, 7), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);
        c_buff[0] = 10;
        c_buff[1] = 10;
        wb.putPage(RaftLogReader::toFullPageId(10, 10), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);

        c_buff[0] = 100;
        c_buff[1] = 1;
        wb.putPage(RaftLogReader::toFullPageId(100, 7), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);

        wb.putPage(RaftLogReader::toFullPageId(255, 10), tag, std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz), buf_sz);

        page_storage->write(std::move(wb));
    }

    RaftLogReader raft_log_reader(*page_storage);
    auto checker = [this](const DB::UniversalPage & page) {
        LOG_INFO(log, "{}", page.page_id);
    };
    raft_log_reader.traverse(RaftLogReader::toFullPageId(10, 0), RaftLogReader::toFullPageId(101, 0), checker);
}
} // namespace PS::universal::tests
} // namespace DB
