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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/RandomData.h>
#include <Common/TiFlashMetrics.h>
#include <IO/Checksum/ChecksumBuffer.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Poco/Path.h>
#include <Server/DTTool/DTTool.h>
#include <Server/RaftConfigParser.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/PathPool.h>
#include <boost_wrapper/program_options.h>
#include <common/defines.h>
#include <pingcap/Config.h>

#include <boost/program_options/errors.hpp>
#include <boost/throw_exception.hpp>
#include <chrono>
#include <iostream>
#include <random>
#include <utility>
namespace bpo = boost::program_options;

namespace DTTool::Bench
{

using namespace DB::DM;
using namespace DB;
std::unique_ptr<Context> global_context = nullptr;

ColumnDefinesPtr getDefaultColumns()
{
    // Return [handle, ver, del] column defines
    ColumnDefinesPtr columns = std::make_shared<ColumnDefines>(ColumnDefines{
        getExtraHandleColumnDefine(/*is_common_handle=*/false),
        getVersionColumnDefine(),
        getTagColumnDefine(),
    });
    return columns;
}

ColumnDefinesPtr createColumnDefines(size_t column_number)
{
    auto primitive = getDefaultColumns();
    auto int_num = column_number / 2;
    auto str_num = column_number - int_num;
    for (size_t i = 0; i < int_num; ++i)
    {
        primitive->emplace_back(ColumnDefine{
            static_cast<ColId>(3 + i),
            fmt::format("int_{}", i),
            DB::DataTypeFactory::instance().get("Int64")});
    }
    for (size_t i = 0; i < str_num; ++i)
    {
        primitive->emplace_back(ColumnDefine{
            static_cast<ColId>(3 + int_num + i),
            fmt::format("str_{}", i),
            DB::DataTypeFactory::instance().get("Nullable(String)")});
    }
    return primitive;
}

DB::Block createBlock(
    size_t column_number,
    size_t start,
    size_t row_number,
    std::size_t limit,
    double sparse_ratio,
    std::mt19937_64 & eng,
    size_t & acc,
    const LoggerPtr & logger)
{
    using namespace DB;
    auto int_num = column_number / 2;
    auto str_num = column_number - int_num;
    Block block;
    //PK
    {
        ColumnWithTypeAndName pk_col(nullptr, EXTRA_HANDLE_COLUMN_INT_TYPE, "id", EXTRA_HANDLE_COLUMN_ID);
        IColumn::MutablePtr m_col = pk_col.type->createColumn();
        for (size_t i = 0; i < row_number; i++)
        {
            Field field = static_cast<DB::Int64>(start + i);
            m_col->insert(field);
            acc += 8;
        }
        pk_col.column = std::move(m_col);
        block.insert(std::move(pk_col));
    }
    // Version
    {
        ColumnWithTypeAndName version_col({}, VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME, VERSION_COLUMN_ID);
        IColumn::MutablePtr m_col = version_col.type->createColumn();
        for (size_t i = 0; i < row_number; ++i)
        {
            Field field = static_cast<DB::UInt64>((start + i) * 10);
            m_col->insert(field);
            acc += 8;
        }
        version_col.column = std::move(m_col);
        block.insert(std::move(version_col));
    }

    //Tag
    {
        ColumnWithTypeAndName tag_col(nullptr, TAG_COLUMN_TYPE, TAG_COLUMN_NAME, TAG_COLUMN_ID);
        IColumn::MutablePtr m_col = tag_col.type->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt8> &>(*m_col).getData();
        column_data.resize(row_number);
        for (size_t i = 0; i < row_number; ++i)
        {
            column_data[i] = eng() & 1;
            acc += 1;
        }
        tag_col.column = std::move(m_col);
        block.insert(std::move(tag_col));
    }

    std::uniform_int_distribution<Int64> int_dist;
    for (size_t i = 0; i < int_num; ++i)
    {
        ColumnWithTypeAndName int_col(
            nullptr,
            DB::DataTypeFactory::instance().get("Int64"),
            fmt::format("int_{}", i),
            static_cast<ColId>(3 + i));
        IColumn::MutablePtr m_col = int_col.type->createColumn();
        auto & column_data = typeid_cast<ColumnVector<Int64> &>(*m_col).getData();
        column_data.resize(row_number);
        for (size_t j = 0; j < row_number; ++j)
        {
            column_data[j] = int_dist(eng);
            acc += 8;
        }
        int_col.column = std::move(m_col);
        block.insert(std::move(int_col));
    }

    std::uniform_real_distribution<> real_dist(0.0, 1.0);
    for (size_t i = 0; i < str_num; ++i)
    {
        String col_name = fmt::format("str_{}", i);
        ColumnWithTypeAndName str_col(
            nullptr,
            DB::DataTypeFactory::instance().get("Nullable(String)"),
            col_name,
            static_cast<ColId>(3 + int_num + i));
        IColumn::MutablePtr m_col = str_col.type->createColumn();
        size_t num_null = 0;
        for (size_t j = 0; j < row_number; j++)
        {
            bool is_null = false;
            if (sparse_ratio > 0.0 && real_dist(eng) < sparse_ratio)
                is_null = true;
            if (is_null)
            {
                m_col->insertDefault();
                num_null++;
            }
            else
            {
                Field field = DB::random::randomString(limit);
                m_col->insert(field);
            }
        }
        str_col.column = std::move(m_col);
        block.insert(std::move(str_col));
        if (sparse_ratio > 0.0)
        {
            LOG_TRACE(
                logger,
                "Sparse_ratio={} column_name={} num_null={} num_rows={}",
                sparse_ratio,
                col_name,
                num_null,
                row_number);
        }
    }

    return block;
}

std::tuple<std::vector<DB::Block>, std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty>, size_t> //
genBlocks(
    size_t random,
    const size_t num_rows,
    const size_t num_column,
    size_t field,
    double sparse_ratio,
    const LoggerPtr & logger)
{
    std::vector<DB::Block> blocks;
    std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty> properties;
    size_t effective_size = 0;

    auto engine = std::mt19937_64{random};
    auto num_blocks = static_cast<size_t>(std::round(1.0 * num_rows / DEFAULT_MERGE_BLOCK_SIZE));
    for (size_t i = 0, count = 1, start_handle = 0; i < num_blocks; ++i)
    {
        auto block_size = DEFAULT_MERGE_BLOCK_SIZE;
        LOG_INFO(logger, "generating block with size: {}", block_size);
        blocks.push_back(DTTool::Bench::createBlock(
            num_column,
            start_handle,
            block_size,
            field,
            sparse_ratio,
            engine,
            effective_size,
            logger));
        start_handle += block_size;
        DB::DM::DMFileBlockOutputStream::BlockProperty property{};
        property.gc_hint_version = count;
        property.effective_num_rows = block_size;
        properties.push_back(property);
    }
    LOG_INFO(
        logger,
        "Blocks generated, num_rows={} num_blocks={} num_column={} effective_size={}",
        num_rows,
        num_blocks,
        num_column,
        effective_size);
    return {std::move(blocks), std::move(properties), effective_size};
}


int benchEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge IO Bench"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool encryption;
    // clang-format off
    options.add_options()
        ("help", "Print help message and exit.")
        ("version", bpo::value<size_t>()->default_value(2), "DTFile version. [available: 1, 2, 3]")
        ("algorithm", bpo::value<std::string>()->default_value("xxh3"), "Checksum algorithm. [available: xxh3, city128, crc32, crc64, none]")
        ("frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE), "Checksum frame length.")
        ("rows", bpo::value<size_t>()->default_value(131072), "Row number.")
        ("columns", bpo::value<size_t>()->default_value(100), "Column number.")
        ("sparse-ratio", bpo::value<double>()->default_value(0.0), "Sparse ratio. Null ratio for string columns.")
        ("field", bpo::value<size_t>()->default_value(1024), "Field length limit.")
        ("repeat", bpo::value<size_t>()->default_value(5), "Repeat times.")
        ("write-repeat", bpo::value<size_t>()->default_value(5), "Write repeat times, 0 means no write operation.")
        ("random", bpo::value<size_t>(), "Random seed. If not set, a random seed will be generated.")
        ("encryption", bpo::bool_switch(&encryption), "Enable encryption.")
        ("workdir", bpo::value<String>()->default_value("/tmp/test"), "Directory to create temporary data storage.")
        ("clean", bpo::bool_switch(), "Clean up the workdir after the bench is done. If false, the workdir will not be cleaned up, please clean it manually if needed.");
    ;
    // clang-format on

    try
    {
        bpo::store(
            bpo::command_line_parser(opts)
                .options(options)
                .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                .run(),
            vm);

        bpo::notify(vm);
    }
    catch (const boost::wrapexcept<boost::program_options::unknown_option> & e)
    {
        std::cerr << e.what() << std::endl;
        options.print(std::cerr);
        return -EINVAL;
    }

    if (vm.count("help") != 0)
    {
        options.print(std::cerr);
        return 0;
    }

    try
    {
        auto version = vm["version"].as<size_t>();
        auto algorithm_config = vm["algorithm"].as<std::string>();
        DB::ChecksumAlgo algorithm;
        if (algorithm_config == "xxh3")
        {
            algorithm = DB::ChecksumAlgo::XXH3;
        }
        else if (algorithm_config == "crc32")
        {
            algorithm = DB::ChecksumAlgo::CRC32;
        }
        else if (algorithm_config == "crc64")
        {
            algorithm = DB::ChecksumAlgo::CRC64;
        }
        else if (algorithm_config == "city128")
        {
            algorithm = DB::ChecksumAlgo::City128;
        }
        else if (algorithm_config == "none")
        {
            algorithm = DB::ChecksumAlgo::None;
        }
        else
        {
            std::cerr << "invalid algorithm: " << algorithm_config << std::endl;
            return -EINVAL;
        }
        auto frame = vm["frame"].as<size_t>();
        auto num_rows = vm["rows"].as<size_t>();
        auto num_cols = vm["columns"].as<size_t>();
        auto sparse_ratio = vm["sparse-ratio"].as<double>();
        auto field = vm["field"].as<size_t>();
        auto repeat = vm["repeat"].as<size_t>();
        auto write_repeat = vm["write-repeat"].as<size_t>();
        size_t random_seed;
        if (vm.count("random"))
        {
            random_seed = vm["random"].as<size_t>();
        }
        else
        {
            random_seed = std::random_device{}();
        }
        auto workdir = vm["workdir"].as<std::string>() + "/.tmp";
        bool clean = vm["clean"].as<bool>();
        if (write_repeat == 0)
            clean = false;
        auto env = detail::ImitativeEnv{workdir, encryption};

        // env is up, use logger from now on
        auto logger = Logger::get();
        SCOPE_EXIT({
            // Cleanup the workdir after the bench is done
            if (clean)
            {
                if (Poco::File file(workdir); file.exists())
                {
                    file.remove(true);
                }
            }
            else
            {
                LOG_INFO(logger, "Workdir {} is not cleaned up, please clean it manually if needed", workdir);
            }
        });

        static constexpr char SUMMARY_TEMPLATE_V2[] = "version: {} "
                                                      "column: {} "
                                                      "num_rows: {} "
                                                      "field: {} "
                                                      "random: {} "
                                                      "encryption: {} "
                                                      "workdir: {} "
                                                      "frame: {} "
                                                      "algorithm: {} ";
        DB::DM::DMConfigurationOpt opt = std::nullopt;
        if (version == 1)
        {
            LOG_INFO(
                logger,
                SUMMARY_TEMPLATE_V2,
                version,
                num_cols,
                num_rows,
                field,
                random_seed,
                encryption,
                workdir,
                "none",
                "none");
            DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V2;
        }
        else
        {
            LOG_INFO(
                logger,
                SUMMARY_TEMPLATE_V2,
                version,
                num_cols,
                num_rows,
                field,
                random_seed,
                encryption,
                workdir,
                frame,
                algorithm_config);
            opt.emplace(std::map<std::string, std::string>{}, frame, algorithm);
            if (version == 2)
            {
                // frame checksum
                DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V3;
            }
            else if (version == 3)
            {
                // DMFileMetaV2
                DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V5;
            }
            else
            {
                std::cerr << "invalid dtfile version: " << version << std::endl;
                return -EINVAL;
            }
        }

        // start initialization
        size_t effective_size = 0;
        auto defines = DTTool::Bench::createColumnDefines(num_cols);
        std::vector<DB::Block> blocks;
        std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty> properties;
        if (write_repeat > 0)
        {
            std::tie(blocks, properties, effective_size)
                = genBlocks(random_seed, num_rows, num_cols, field, sparse_ratio, logger);
        }

        TableID table_id = 1;
        auto settings = DB::Settings();
        auto db_context = env.getContext();
        auto path_pool
            = std::make_shared<DB::StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        auto storage_pool
            = std::make_shared<DB::DM::StoragePool>(*db_context, NullspaceID, table_id, *path_pool, "test.t1");
        auto dm_settings = DB::DM::DeltaMergeStore::Settings{};
        auto dm_context = DB::DM::DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            table_id,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
        DB::DM::DMFilePtr dmfile = nullptr;

        UInt64 file_id = 1;

        // Write
        if (write_repeat > 0)
        {
            size_t write_cost_ms = 0;
            LOG_INFO(logger, "start writing");
            for (size_t i = 0; i < write_repeat; ++i)
            {
                using namespace std::chrono;
                dmfile = DB::DM::DMFile::create(file_id, workdir, opt);
                auto start = high_resolution_clock::now();
                {
                    auto stream = DB::DM::DMFileBlockOutputStream(*db_context, dmfile, *defines);
                    stream.writePrefix();
                    for (size_t j = 0; j < blocks.size(); ++j)
                    {
                        stream.write(blocks[j], properties[j]);
                    }
                    stream.writeSuffix();
                }
                auto end = high_resolution_clock::now();
                auto duration = duration_cast<milliseconds>(end - start).count();
                write_cost_ms += duration;
                LOG_INFO(logger, "attempt {} finished in {} ms", i, duration);
            }
            size_t effective_size_on_disk = dmfile->getBytesOnDisk();
            LOG_INFO(
                logger,
                "average write time: {} ms",
                (static_cast<double>(write_cost_ms) / static_cast<double>(repeat)));
            LOG_INFO(
                logger,
                "write throughput by uncompressed size: {:.3f}MiB/s;"
                " write throughput by compressed size: {:.3f}MiB/s",
                (effective_size * 1'000.0 * repeat / write_cost_ms / 1024 / 1024),
                (effective_size_on_disk * 1'000.0 * repeat / write_cost_ms / 1024 / 1024));
        }

        // Read
        dmfile
            = DB::DM::DMFile::restore(db_context->getFileProvider(), file_id, 0, workdir, DMFileMeta::ReadMode::all());
        if (!dmfile)
        {
            LOG_ERROR(logger, "Failed to restore DMFile with file_id={}", file_id);
            return -ENOENT;
        }

        size_t effective_size_read = dmfile->getBytes();
        size_t effective_size_on_disk = dmfile->getBytesOnDisk();
        LOG_INFO(
            logger,
            "start reading, effective_size={}, effective_size_on_disk={}",
            effective_size_read,
            effective_size_on_disk);
        size_t read_cost_ms = 0;
        for (size_t i = 0; i < repeat; ++i)
        {
            using namespace std::chrono;

            auto start = high_resolution_clock::now();
            {
                auto builder = DB::DM::DMFileBlockInputStreamBuilder(*db_context);
                auto stream = builder.setColumnCache(std::make_shared<DB::DM::ColumnCache>())
                                  .build(
                                      dmfile,
                                      *defines,
                                      {DB::DM::RowKeyRange::newAll(false, 1)},
                                      std::make_shared<ScanContext>());
                while (true)
                {
                    auto block = stream->read();
                    if (!block)
                        break;
                    TIFLASH_NO_OPTIMIZE(block);
                }
                stream->readSuffix();
            }
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<milliseconds>(end - start).count();
            read_cost_ms += duration;
            LOG_INFO(logger, "attempt {} finished in {} ms", i, duration);
        }

        LOG_INFO(logger, "average read time: {} ms", (static_cast<double>(read_cost_ms) / static_cast<double>(repeat)));
        LOG_INFO(
            logger,
            "read throughput by uncompressed bytes: {:.3f}MiB/s;"
            " read throughput by compressed bytes: {:.3f}MiB/s",
            (effective_size_read * 1'000.0 * repeat / read_cost_ms / 1024 / 1024),
            (effective_size_on_disk * 1'000.0 * repeat / read_cost_ms / 1024 / 1024));
    }
    catch (const boost::wrapexcept<boost::bad_any_cast> & e)
    {
        std::cerr << "invalid argument: " << e.what() << std::endl;
        options.print(std::cerr); // no env available here
        return -EINVAL;
    }
    catch (...)
    {
        tryLogCurrentException(Logger::get(), "DTToolBench");
    }

    return 0;
}

} // namespace DTTool::Bench
