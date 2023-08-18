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

#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashStorageTestBasic.h>

namespace DB::DM::Remote::tests
{

class LocalPageCacheTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        auto path = getTemporaryPath();
        dropDataOnDisk(path);
        createIfNotExist(path);
        auto file_provider = DB::tests::TiFlashTestEnv::getDefaultFileProvider();
        auto delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);
        page_storage = UniversalPageStorage::create("cache_store", delegator, {}, file_provider);
        page_storage->restore();
    }

protected:
    std::shared_ptr<UniversalPageStorage> page_storage;
};

TEST_F(LocalPageCacheTest, ReadWriteWithoutMaxSize)
try
{
    RNLocalPageCache cache({.underlying_storage = page_storage});
    cache.write({.page_id = 1}, "page_1_data", {11});
    {
        auto page = cache.getPage({.page_id = 1}, {0});
        ASSERT_EQ("page_1_data", page.getFieldData(0));
    }
    ASSERT_THROW({ cache.getPage({.page_id = 2}, {0}); }, DB::Exception);

    ASSERT_THROW({ cache.getPage({.page_id = 1}, {1}); }, DB::Exception);

    cache.write({.page_id = 5}, "foo_bar", {3, 4});
    {
        auto page = cache.getPage({.page_id = 5}, {0});
        ASSERT_EQ("foo", page.getFieldData(0));
    }
    {
        auto page = cache.getPage({.page_id = 5}, {1});
        ASSERT_EQ("_bar", page.getFieldData(1));
    }
}
CATCH

} // namespace DB::DM::Remote::tests
