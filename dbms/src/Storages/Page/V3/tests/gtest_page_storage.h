// Copyright 2023 PingCAP, Inc.
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

#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

namespace DB
{
namespace PS::V3::tests
{
class PageStorageTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        for (size_t i = 0; i < buf_sz; ++i)
            c_buff[i] = i % 0xff;

        log = Logger::get();
        auto path = getTemporaryPath();
        createIfNotExist(path);
        file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = std::make_shared<PageStorageImpl>(String(NAME), delegator, config, file_provider);
        page_storage->restore();
    }

    std::shared_ptr<PageStorageImpl> reopenWithConfig(const PageStorageConfig & config_)
    {
        auto path = getTemporaryPath();
        delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        auto storage = std::make_shared<PageStorageImpl>(String(NAME), delegator, config_, file_provider);
        storage->restore();
        return storage;
    }

    size_t getLogFileNum()
    {
        auto log_files = WALStoreReader::listAllFiles(delegator, log);
        return log_files.size();
    }

    ReadBufferPtr getDefaultBuffer() const { return std::make_shared<ReadBufferFromMemory>(c_buff, buf_sz); }

protected:
    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    PageStorageConfig config;
    std::shared_ptr<PageStorageImpl> page_storage;

    static constexpr std::string_view NAME{"test.t"};
    DB::UInt64 default_tag = 0;
    constexpr static size_t buf_sz = 1024;
    char c_buff[buf_sz];

    LoggerPtr log;
};
} // namespace PS::V3::tests
} // namespace DB
