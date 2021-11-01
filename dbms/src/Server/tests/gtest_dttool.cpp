#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/PathPool.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>

#include <ctime>
#include <random>
namespace DTTool::Bench
{
using namespace DB::DM;
using namespace DB;

ColumnDefinesPtr getDefaultColumns();
Context getContext(const DB::Settings & settings, const String & tmp_path);
ColumnDefinesPtr createColumnDefines(size_t column_number);
Block createBlock(size_t column_number, size_t start, size_t row_number, std::size_t limit, std::mt19937_64 & eng, size_t & acc);
} // namespace DTTool::Bench

namespace DTTool::Inspect
{
int inspectServiceMain(DB::Context & context, const InspectArgs & args);
} // namespace DTTool::Inspect

struct DTToolTest : public DB::base::TiFlashStorageTestBasic
{
    DB::DM::DMFilePtr dmfile = nullptr;
    static constexpr size_t column = 64;
    static constexpr size_t size = 128;
    static constexpr size_t field = 512;

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        using namespace DTTool::Bench;

        auto dev = std::random_device{};
        auto seed = dev();
        auto engine = std::mt19937_64{seed};
        auto defines = DTTool::Bench::createColumnDefines(column);
        std::vector<DB::Block> blocks;
        std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty> properties;
        size_t effective_size = 0;
        for (size_t i = 0, count = 1; i < size; count++)
        {
            auto block_size = engine() % (size - i) + 1;
            blocks.push_back(DTTool::Bench::createBlock(column, i, block_size, field, engine, effective_size));
            i += block_size;
            DB::DM::DMFileBlockOutputStream::BlockProperty property{};
            property.gc_hint_version = count;
            property.effective_num_rows = block_size;
            properties.push_back(property);
        }
        auto path_pool = std::make_unique<DB::StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        auto storage_pool = std::make_unique<DB::DM::StoragePool>("test.t1", *path_pool, *db_context, db_context->getSettingsRef());
        auto dm_settings = DB::DM::DeltaMergeStore::Settings{};
        auto dm_context = std::make_unique<DB::DM::DMContext>( //
            *db_context,
            *path_pool,
            *storage_pool,
            /*hash_salt*/ 0,
            0,
            dm_settings.not_compress_columns,
            false,
            1,
            db_context->getSettingsRef());
        // Write
        {
            dmfile = DB::DM::DMFile::create(1, getTemporaryPath(), false, std::nullopt);
            {
                auto stream = DB::DM::DMFileBlockOutputStream(*db_context, dmfile, *defines);
                stream.writePrefix();
                for (size_t j = 0; j < blocks.size(); ++j)
                {
                    stream.write(blocks[j], properties[j]);
                }
                stream.writeSuffix();
            }
        }
    }
};


TEST_F(DTToolTest, MigrationAllFileRecognizableOnDefault)
{
    std::vector<std::string> sub_files;
    Poco::File(dmfile->path()).list(sub_files);
    for (auto & i : sub_files)
    {
        EXPECT_TRUE(DTTool::Migrate::isRecognizable(*dmfile, i)) << " file: " << i;
    }
}

TEST_F(DTToolTest, MigrationSuccess)
{
    {
        auto args = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 1,
            .version = 2,
            .frame = DBMS_DEFAULT_BUFFER_SIZE,
            .algorithm = DB::ChecksumAlgo::XXH3,
            .workdir = getTemporaryPath()};

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, args), 0);
    }
    {
        auto args = DTTool::Inspect::InspectArgs{
            .check = true,
            .file_id = 1,
            .workdir = getTemporaryPath()};
        EXPECT_EQ(DTTool::Inspect::inspectServiceMain(*db_context, args), 0);
    }
}