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

#include <Common/Checksum.h>
#include <Common/Logger.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>

#include <ctime>
#include <fstream>
#include <random>
namespace DTTool::Bench
{
using namespace DB::DM;
using namespace DB;

ColumnDefinesPtr getDefaultColumns();
Context getContext(const DB::Settings & settings, const String & tmp_path);
ColumnDefinesPtr createColumnDefines(size_t column_number);
Block createBlock(
    size_t column_number,
    size_t start,
    size_t row_number,
    std::size_t limit,
    std::mt19937_64 & eng,
    size_t & acc);
} // namespace DTTool::Bench

namespace DTTool::Inspect
{
int inspectServiceMain(DB::Context & context, const InspectArgs & args);
} // namespace DTTool::Inspect

struct DTToolTest : public DB::base::TiFlashStorageTestBasic
{
    DB::DM::DMFilePtr dmfile = nullptr;
    DB::DM::DMFilePtr dmfileV3 = nullptr;
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
        auto path_pool
            = std::make_shared<DB::StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        auto storage_pool
            = std::make_shared<DB::DM::StoragePool>(*db_context, NullspaceID, /*ns_id*/ 1, *path_pool, "test.t1");
        auto dm_settings = DB::DM::DeltaMergeStore::Settings{};
        auto dm_context = std::make_unique<DB::DM::DMContext>( //
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 1,
            false,
            1,
            db_context->getSettingsRef());
        // Write
        {
            dmfile = DB::DM::DMFile::create(1, getTemporaryPath(), std::nullopt, 0, 0, DMFileFormat::V0);
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

        // Write DMFile::V3
        {
            dmfileV3 = DB::DM::DMFile::create(
                2,
                getTemporaryPath(),
                std::make_optional<DMChecksumConfig>(),
                128 * 1024,
                16 * 1024 * 1024,
                DMFileFormat::V3);
            {
                auto stream = DB::DM::DMFileBlockOutputStream(*db_context, dmfileV3, *defines);
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

    Poco::File(dmfileV3->path()).list(sub_files);
    for (auto & i : sub_files)
    {
        EXPECT_TRUE(DTTool::Migrate::isRecognizable(*dmfileV3, i)) << " file: " << i;
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
            .workdir = getTemporaryPath(),
            .compression_method = DB::CompressionMethod::LZ4,
            .compression_level = DB::CompressionSettings::getDefaultLevel(DB::CompressionMethod::LZ4),
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, args), 0);
    }
    {
        auto args = DTTool::Inspect::InspectArgs{.check = true, .file_id = 1, .workdir = getTemporaryPath()};
        EXPECT_EQ(DTTool::Inspect::inspectServiceMain(*db_context, args), 0);
    }
}


TEST_F(DTToolTest, MigrationV3toV2Success)
{
    {
        auto args = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 2,
            .version = 2,
            .frame = DBMS_DEFAULT_BUFFER_SIZE,
            .algorithm = DB::ChecksumAlgo::XXH3,
            .workdir = getTemporaryPath(),
            .compression_method = DB::CompressionMethod::LZ4,
            .compression_level = DB::CompressionSettings::getDefaultLevel(DB::CompressionMethod::LZ4),
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, args), 0);
    }
    {
        auto args = DTTool::Inspect::InspectArgs{.check = true, .file_id = 2, .workdir = getTemporaryPath()};
        EXPECT_EQ(DTTool::Inspect::inspectServiceMain(*db_context, args), 0);
    }
}

void getHash(std::unordered_map<std::string, std::string> & records, const std::string & path)
{
    std::fstream file{path};
    auto digest = DB::UnifiedDigest<DB::Digest::CRC64>{};
    std::vector<char> buffer(DBMS_DEFAULT_BUFFER_SIZE);
    while (auto length = file.readsome(buffer.data(), buffer.size()))
    {
        digest.update(buffer.data(), length);
    }
    records[path] = digest.raw();
}

void compareHash(std::unordered_map<std::string, std::string> & records)
{
    for (const auto & i : records)
    {
        std::fstream file{i.first};
        auto digest = DB::UnifiedDigest<DB::Digest::CRC64>{};
        std::vector<char> buffer(DBMS_DEFAULT_BUFFER_SIZE);
        while (auto length = file.readsome(buffer.data(), buffer.size()))
        {
            digest.update(buffer.data(), length);
        }
        EXPECT_TRUE(digest.compareRaw(i.second)) << "file: " << i.first;
    }
}

TEST_F(DTToolTest, ConsecutiveMigration)
{
    auto args = DTTool::Migrate::MigrateArgs{
        .no_keep = false,
        .dry_mode = false,
        .file_id = 1,
        .version = 1,
        .frame = DBMS_DEFAULT_BUFFER_SIZE,
        .algorithm = DB::ChecksumAlgo::XXH3,
        .workdir = getTemporaryPath(),
        .compression_method = DB::CompressionMethod::LZ4,
        .compression_level = DB::CompressionSettings::getDefaultLevel(DB::CompressionMethod::LZ4),
    };

    EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, args), 0);
    auto logger = DB::Logger::get("DTToolTest");
    std::unordered_map<std::string, std::string> records;
    {
        Poco::File file{dmfile->path()};
        std::vector<std::string> subfiles;
        file.list(subfiles);
        for (const auto & i : subfiles)
        {
            if (!DTTool::Migrate::needFrameMigration(*dmfile, i))
                continue;
            LOG_INFO(logger, "record file: {}", i);
            getHash(records, i);
        }
    }
    std::vector<std::tuple<size_t, DB::ChecksumAlgo, DB::CompressionMethod, int>> test_cases{
        {2, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::LZ4, -1},
        {1, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::ZSTD, 1},
        {2, DB::ChecksumAlgo::City128, DB::CompressionMethod::LZ4HC, 0},
        {2, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {args.version, args.algorithm, args.compression_method, args.compression_level}};
    for (auto [version, algo, comp, level] : test_cases)
    {
        auto a = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 1,
            .version = version,
            .frame = DBMS_DEFAULT_BUFFER_SIZE,
            .algorithm = algo,
            .workdir = getTemporaryPath(),
            .compression_method = comp,
            .compression_level = level,
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, a), 0);
    }

    compareHash(records);
}

TEST_F(DTToolTest, BlockwiseInvariant)
{
    std::vector<size_t> size_info{};
    {
        auto stream = DB::DM::createSimpleBlockInputStream(*db_context, dmfile);
        stream->readPrefix();
        while (auto block = stream->read())
        {
            size_info.push_back(block.bytes());
        }
        stream->readSuffix();
    }

    std::vector<std::tuple<size_t, size_t, DB::ChecksumAlgo, DB::CompressionMethod, int>> test_cases{
        {2, DBMS_DEFAULT_BUFFER_SIZE, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::LZ4, -1},
        {1, 64, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::ZSTD, 1},
        {2, DBMS_DEFAULT_BUFFER_SIZE * 2, DB::ChecksumAlgo::City128, DB::CompressionMethod::LZ4HC, 0},
        {2, DBMS_DEFAULT_BUFFER_SIZE * 4, DB::ChecksumAlgo::City128, DB::CompressionMethod::LZ4HC, 0},
        {2, 4, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {2, 13, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {2, 5261, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {1, DBMS_DEFAULT_BUFFER_SIZE, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::NONE, -1}};
    for (auto [version, frame_size, algo, comp, level] : test_cases)
    {
        auto a = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 1,
            .version = version,
            .frame = frame_size,
            .algorithm = algo,
            .workdir = getTemporaryPath(),
            .compression_method = comp,
            .compression_level = level,
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, a), 0);
        auto refreshed_file = DB::DM::DMFile::restore(
            db_context->getFileProvider(),
            1,
            0,
            getTemporaryPath(),
            DB::DM::DMFileMeta::ReadMode::all());
        if (version == 2)
        {
            EXPECT_EQ(refreshed_file->getConfiguration()->getChecksumFrameLength(), frame_size);
        }
        auto stream = DB::DM::createSimpleBlockInputStream(*db_context, refreshed_file);
        auto size_iter = size_info.begin();
        auto prop_iter = dmfile->getPackProperties().property().begin();
        auto new_prop_iter = refreshed_file->getPackProperties().property().begin();
        auto stat_iter = dmfile->getPackStats().begin();
        auto new_stat_iter = refreshed_file->getPackStats().begin();
        stream->readPrefix();
        while (auto block = stream->read())
        {
            EXPECT_EQ(*size_iter++, block.bytes());
            EXPECT_EQ(prop_iter->gc_hint_version(), new_prop_iter->gc_hint_version());
            EXPECT_EQ(prop_iter->num_rows(), new_prop_iter->num_rows());
            EXPECT_EQ(stat_iter->rows, new_stat_iter->rows);
            EXPECT_EQ(stat_iter->not_clean, new_stat_iter->not_clean);
            EXPECT_EQ(stat_iter->first_version, new_stat_iter->first_version);
            EXPECT_EQ(stat_iter->bytes, new_stat_iter->bytes);
            EXPECT_EQ(stat_iter->first_tag, new_stat_iter->first_tag);
            prop_iter++;
            new_prop_iter++;
            stat_iter++;
            new_stat_iter++;
        }
        EXPECT_EQ(stat_iter, dmfile->getPackStats().end());
        EXPECT_EQ(new_stat_iter, refreshed_file->getPackStats().end());
        EXPECT_EQ(prop_iter, dmfile->getPackProperties().property().end());
        EXPECT_EQ(new_prop_iter, refreshed_file->getPackProperties().property().end());
        stream->readSuffix();
    }
}
