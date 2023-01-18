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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>

#include <vector>

namespace DB
{
namespace DM
{
namespace tests
{
class ColumnFileTest
    : public DB::base::TiFlashStorageTestBasic
{
public:
    ColumnFileTest() = default;

    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "DMFile_Test", false));
        storage_pool = std::make_unique<StoragePool>(*db_context, /*ns_id*/ 100, *path_pool, "test.t1");
        column_cache = std::make_shared<ColumnCache>();
        dm_context = std::make_unique<DMContext>( //
            *db_context,
            *path_pool,
            *storage_pool,
            0,
            /*min_version_*/ 0,
            settings.not_compress_columns,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMContext & dmContext() { return *dm_context; }

    Context & dbContext() { return *db_context; }

private:
    std::unique_ptr<DMContext> dm_context;
    /// all these var live as ref in dm_context
    std::unique_ptr<StoragePathPool> path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    DeltaMergeStore::Settings settings;

protected:
    String parent_path;
    ColumnCachePtr column_cache;
};

TEST_F(ColumnFileTest, SerializeColumnFilePersisted)
try
{
    WriteBatches wbs(dmContext().storage_pool, dmContext().getWriteLimiter());
    MemoryWriteBuffer buff;
    {
        ColumnFilePersisteds column_file_persisteds;
        size_t rows = 100; // arbitrary value
        auto block = DMTestEnv::prepareSimpleWriteBlock(0, rows, false);
        auto schema = std::make_shared<Block>(block.cloneEmpty());
        column_file_persisteds.push_back(ColumnFileTiny::writeColumnFile(dmContext(), block, 0, rows, wbs, schema));
        column_file_persisteds.emplace_back(std::make_shared<ColumnFileDeleteRange>(RowKeyRange::newAll(false, 1)));
        column_file_persisteds.push_back(ColumnFileTiny::writeColumnFile(dmContext(), block, 0, rows, wbs, schema));
        column_file_persisteds.emplace_back(std::make_shared<ColumnFileDeleteRange>(RowKeyRange::newAll(false, 1)));
        column_file_persisteds.push_back(ColumnFileTiny::writeColumnFile(dmContext(), block, 0, rows, wbs, schema));
        serializeSavedColumnFilesInV3Format(buff, column_file_persisteds);
    }

    {
        auto read_buff = buff.tryGetReadBuffer();
        auto column_file_persisteds = deserializeSavedColumnFilesInV3Format(dmContext(), RowKeyRange::newAll(false, 1), *read_buff);
        ASSERT_EQ(column_file_persisteds.size(), 5);
        ASSERT_EQ(column_file_persisteds[0]->tryToTinyFile()->getSchema(), column_file_persisteds[2]->tryToTinyFile()->getSchema());
        ASSERT_EQ(column_file_persisteds[2]->tryToTinyFile()->getSchema(), column_file_persisteds[4]->tryToTinyFile()->getSchema());
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
