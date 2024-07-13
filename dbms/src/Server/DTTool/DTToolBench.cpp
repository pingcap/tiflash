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

#include <Common/TiFlashMetrics.h>
#include <Encryption/MockKeyManager.h>
#include <IO/ChecksumBuffer.h>
#include <Poco/Path.h>
#include <Server/DTTool/DTTool.h>
#include <Server/RaftConfigParser.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/PathPool.h>
#include <pingcap/Config.h>

#include <boost/program_options.hpp>
#include <chrono>
#include <iostream>
#include <random>
#include <utility>
namespace bpo = boost::program_options;

namespace DTTool::Bench
{
// clang-format off
static constexpr char BENCH_HELP[] =
    "Usage: bench [args]\n"
    "Available Arguments:\n"
    "  --help        Print help message and exit.\n"
    "  --version     DTFile version. [default: 2] [available: 1, 2]\n"
    "  --algorithm   Checksum algorithm. [default: xxh3] [available: xxh3, city128, crc32, crc64, none]\n"
    "  --frame       Checksum frame length. [default: " TO_STRING(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE) "]\n"
    "  --column      Column number. [default: 100]\n"
    "  --size        Column size.   [default: 1000]\n"
    "  --field       Field length limit. [default: 1024]\n"
    "  --random      Random seed. (optional)\n"
    "  --encryption  Enable encryption.\n"
    "  --repeat      Repeat times. [default: 5]\n"
    "  --workdir     Directory to create temporary data storage. [default: /tmp/test]";
// clang-format on

using namespace DB::DM;
using namespace DB;
std::unique_ptr<Context> global_context = nullptr;

ColumnDefinesPtr getDefaultColumns()
{
    // Return [handle, ver, del] column defines
    ColumnDefinesPtr columns = std::make_shared<ColumnDefines>();
    columns->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
    columns->emplace_back(getVersionColumnDefine());
    columns->emplace_back(getTagColumnDefine());
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
            DB::DataTypeFactory::instance().get("String")});
    }
    return primitive;
}

String createRandomString(std::size_t limit, std::mt19937_64 & eng, size_t & acc)
{
    // libc++-15 forbids instantiate `std::uniform_int_distribution<char>`.
    // see https://github.com/llvm/llvm-project/blob/bfcd536a8ef6b1d6e9dd211925be3b078d06fe77/libcxx/include/__random/is_valid.h#L28
    // and https://github.com/llvm/llvm-project/blob/bfcd536a8ef6b1d6e9dd211925be3b078d06fe77/libcxx/include/__random/uniform_int_distribution.h#L162
    std::uniform_int_distribution<uint8_t> dist('a', 'z');
    std::string buffer((eng() % limit) + 1, 0);
    for (auto & i : buffer)
    {
        i = dist(eng);
    }
    acc += buffer.size();
    return buffer;
}

DB::Block createBlock(
    size_t column_number,
    size_t start,
    size_t row_number,
    std::size_t limit,
    std::mt19937_64 & eng,
    size_t & acc)
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

    std::uniform_int_distribution<Int64> dist;
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
            column_data[j] = dist(eng);
            acc += 8;
        }
        int_col.column = std::move(m_col);
        block.insert(std::move(int_col));
    }

    for (size_t i = 0; i < str_num; ++i)
    {
        ColumnWithTypeAndName str_col(
            nullptr,
            DB::DataTypeFactory::instance().get("String"),
            fmt::format("str_{}", i),
            static_cast<ColId>(3 + int_num + i));
        IColumn::MutablePtr m_col = str_col.type->createColumn();
        for (size_t j = 0; j < row_number; j++)
        {
            Field field = createRandomString(limit, eng, acc);
            m_col->insert(field);
        }
        str_col.column = std::move(m_col);
        block.insert(std::move(str_col));
    }

    return block;
}


int benchEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge IO Bench"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool encryption;
    // clang-format off
    options.add_options()
        ("help", "show help")
        ("version", bpo::value<size_t>()->default_value(2))
        ("algorithm", bpo::value<std::string>()->default_value("xxh3"))
        ("frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE))
        ("column", bpo::value<size_t>()->default_value(100))
        ("size", bpo::value<size_t>()->default_value(1000))
        ("field", bpo::value<size_t>()->default_value(1024))
        ("random", bpo::value<size_t>())
        ("repeat", bpo::value<size_t>()->default_value(5))
        ("encryption", bpo::bool_switch(&encryption))
        ("workdir", bpo::value<String>()->default_value("/tmp"));
    // clang-format on

    bpo::store(
        bpo::command_line_parser(opts)
            .options(options)
            .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
            .run(),
        vm);

    bpo::notify(vm);

    if (vm.count("help") != 0)
    {
        std::cout << BENCH_HELP << std::endl;
        return 0;
    }

    try
    {
        auto version = vm["version"].as<size_t>();
        if (version < 1 || version > 2)
        {
            std::cerr << "invalid dtfile version: " << version << std::endl;
            return -EINVAL;
        }
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
        auto column = vm["column"].as<size_t>();
        auto size = vm["size"].as<size_t>();
        auto field = vm["field"].as<size_t>();
        auto repeat = vm["repeat"].as<size_t>();
        size_t random;
        if (vm.count("random"))
        {
            random = vm["random"].as<size_t>();
        }
        else
        {
            random = std::random_device{}();
        }
        auto workdir = vm["workdir"].as<std::string>() + "/.tmp";
        auto env = detail::ImitativeEnv{workdir, encryption};
        // env is up, use logger from now on
        SCOPE_EXIT({
            if (Poco::File file(workdir); file.exists())
            {
                file.remove(true);
            }
        });
        static constexpr char SUMMARY_TEMPLATE_V1[] = "version:    {}\n"
                                                      "column:     {}\n"
                                                      "size:       {}\n"
                                                      "field:      {}\n"
                                                      "random:     {}\n"
                                                      "encryption: {}\n"
                                                      "workdir:    {}";

        static constexpr char SUMMARY_TEMPLATE_V2[] = "version:    {}\n"
                                                      "column:     {}\n"
                                                      "size:       {}\n"
                                                      "field:      {}\n"
                                                      "random:     {}\n"
                                                      "workdir:    {}\n"
                                                      "frame:      {}\n"
                                                      "encryption: {}\n"
                                                      "algorithm:  {}";
        DB::DM::DMConfigurationOpt opt = std::nullopt;
        auto * logger = &Poco::Logger::get("DTTool::Bench");
        if (version == 1)
        {
            LOG_INFO(logger, SUMMARY_TEMPLATE_V1, version, column, size, field, random, encryption, workdir);
            DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V2;
        }
        else
        {
            LOG_INFO(
                logger,
                SUMMARY_TEMPLATE_V2,
                version,
                column,
                size,
                field,
                random,
                workdir,
                frame,
                encryption,
                algorithm_config);
            opt.emplace(std::map<std::string, std::string>{}, frame, algorithm);
            DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V3;
        }

        // start initialization
        size_t effective_size = 0;
        auto engine = std::mt19937_64{random};
        auto defines = DTTool::Bench::createColumnDefines(column);
        std::vector<DB::Block> blocks;
        std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty> properties;
        for (size_t i = 0, count = 1; i < size; count++)
        {
            auto block_size = engine() % (size - i) + 1;
            LOG_INFO(logger, "generating block with size: {}", block_size);
            blocks.push_back(DTTool::Bench::createBlock(column, i, block_size, field, engine, effective_size));
            i += block_size;
            DB::DM::DMFileBlockOutputStream::BlockProperty property{};
            property.gc_hint_version = count;
            property.effective_num_rows = block_size;
            properties.push_back(property);
        }
        LOG_INFO(logger, "effective_size: {}", effective_size);
        LOG_INFO(logger, "start writing");
        size_t write_records = 0;
        auto settings = DB::Settings();
        auto db_context = env.getContext();
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
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
        DB::DM::DMFilePtr dmfile = nullptr;

        // Write
        for (size_t i = 0; i < repeat; ++i)
        {
            using namespace std::chrono;
            dmfile = DB::DM::DMFile::create(1, workdir, opt);
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
            auto duration = duration_cast<nanoseconds>(end - start).count();
            write_records += duration;
            LOG_INFO(logger, "attemp {} finished in {} ns", i, duration);
        }

        LOG_INFO(
            logger,
            "average write time: {} ns",
            (static_cast<double>(write_records) / static_cast<double>(repeat)));
        LOG_INFO(
            logger,
            "throughput (MB/s): {}",
            (static_cast<double>(effective_size) * 1'000'000'000 * static_cast<double>(repeat)
             / static_cast<double>(write_records) / 1024 / 1024));

        // Read
        LOG_INFO(logger, "start reading");
        size_t read_records = 0;
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
                for (size_t j = 0; j < blocks.size(); ++j)
                {
                    TIFLASH_NO_OPTIMIZE(stream->read());
                }
                stream->readSuffix();
            }
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<nanoseconds>(end - start).count();
            read_records += duration;
            LOG_INFO(logger, "attemp {} finished in {} ns", i, duration);
        }

        LOG_INFO(logger, "average read time: {} ns", (static_cast<double>(read_records) / static_cast<double>(repeat)));
        LOG_INFO(
            logger,
            "throughput (MB/s): {}",
            (static_cast<double>(effective_size) * 1'000'000'000 * static_cast<double>(repeat)
             / static_cast<double>(read_records) / 1024 / 1024));
    }
    catch (const boost::wrapexcept<boost::bad_any_cast> & e)
    {
        std::cerr << BENCH_HELP << std::endl; // no env available here
        return -EINVAL;
    }

    return 0;
}

} // namespace DTTool::Bench
