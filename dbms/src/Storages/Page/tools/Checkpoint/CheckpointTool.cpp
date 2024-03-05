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


#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <Server/StorageConfigParser.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/S3/S3Common.h>
#include <boost_wrapper/program_options.h>

#include <magic_enum.hpp>

using namespace DB;
using namespace DB::PS::V3;
using namespace boost;

namespace DB::PS::CheckpointTool
{

struct Options
{
    String manifest;
    String output;
    StorageS3Config s3_config;

    Options(int argc, char ** argv)
    {
        parse(argc, argv);
        check();
        tryInitS3();
    }

    ~Options() { tryShutdownS3(); }

    void parse(int argc, char * argv[])
    {
        using program_options::value;
        program_options::options_description desc("Allowed options");
        desc.add_options() //
            ("help", "produce help message") //
            ("manifest", value<String>()->default_value(""), "") //
            ("endpoint", value<String>()->default_value(""), "") //
            ("bucket", value<String>()->default_value(""), "") //
            ("access_key_id", value<String>()->default_value(""), "") //
            ("secret_access_key", value<String>()->default_value(""), "") //
            ("root", value<String>()->default_value(""), "") //
            // stat - output statistics only.
            // data - output data only.
            // all - stat + data.
            ("output", value<String>()->default_value("all"), "stat/data/all");

        program_options::variables_map vm;
        program_options::store(program_options::parse_command_line(argc, argv, desc), vm);
        if (vm.count("help") > 0)
        {
            std::cout << desc << std::endl;
            exit(0);
        }

        manifest = vm["manifest"].as<String>();

        if (vm.count("output"))
        {
            output = vm["output"].as<String>();
        }

        s3_config.endpoint = vm["endpoint"].as<String>();
        s3_config.bucket = vm["bucket"].as<String>();
        s3_config.access_key_id = vm["access_key_id"].as<String>();
        s3_config.secret_access_key = vm["secret_access_key"].as<String>();
        s3_config.root = vm["root"].as<String>();
    }

    void check() const
    {
        RUNTIME_CHECK_MSG(!manifest.empty(), "manifest is required");
        RUNTIME_CHECK_MSG(output == "all" || output == "stat" || output == "data", "Not recognize --output={}", output);
    }

    void tryInitS3()
    {
        try
        {
            s3_config.enable(true, Logger::get());
            S3::ClientFactory::instance().init(s3_config);
        }
        catch (...)
        {}
    }

    void tryShutdownS3() const
    {
        if (s3_config.isS3Enabled())
        {
            S3::ClientFactory::instance().shutdown();
        }
    }
};

struct Statistics
{
    std::unordered_map<StorageType, size_t> count_by_storage_type;
    std::unordered_map<EditRecordType, size_t> count_by_record_type;
    size_t dmfile_count = 0;

    void print()
    {
        std::cout << fmt::format("\nStatistics:\n");
        std::cout << fmt::format("DMFile count: {}\n", dmfile_count);

        std::cout << fmt::format("Count by StorageType: ");
        for (const auto & [type, count] : count_by_storage_type)
        {
            std::cout << fmt::format("({}: {}) ", magic_enum::enum_name(type), count);
        }
        std::cout << std::endl;

        std::cout << fmt::format("Count by RecordType: ");
        for (const auto & [type, count] : count_by_record_type)
        {
            std::cout << fmt::format("({}: {}) ", magic_enum::enum_name(type), count);
        }
        std::cout << std::endl;
    }
};

static int run(int argc, char ** argv)
{
    Options opts(argc, argv);

    auto key_manager = std::make_shared<MockKeyManager>(false);
    auto file_provider = std::make_shared<FileProvider>(key_manager, false);
    auto manifest_file = file_provider->newRandomAccessFile(opts.manifest, EncryptionPath(opts.manifest, ""));
    auto manifest_reader = CPManifestFileReader::create({.plain_file = manifest_file});

    std::cout << manifest_reader->readPrefix().DebugString() << std::endl;

    Statistics stat;
    CheckpointProto::StringsInternMap strings_map;
    while (true)
    {
        auto edits = manifest_reader->readEdits(strings_map);
        if (!edits.has_value())
        {
            break;
        }
        const auto & records = edits->getRecords();
        for (const auto & r : records)
        {
            stat.count_by_record_type[r.type]++;
            auto storage_type = UniversalPageIdFormat::getUniversalPageIdType(r.page_id);
            stat.count_by_storage_type[storage_type]++;
            if (opts.output != "stat")
            {
                std::cout << fmt::format(
                    "type:{}, page_id:{}, ori_page_id:{}, being_ref_count:{}, version:{}, entry: {}\n",
                    magic_enum::enum_name(r.type),
                    r.page_id,
                    r.ori_page_id,
                    r.being_ref_count,
                    r.version,
                    r.entry);
            }
        }
    }

    auto locks = manifest_reader->readLocks();
    if (locks.has_value())
    {
        stat.dmfile_count = locks->size();
        if (opts.output != "stat")
        {
            for (const auto & lock : *locks)
            {
                std::cout << lock << std::endl;
            }
        }
    }

    if (opts.output != "data")
    {
        stat.print();
    }

    return 0;
}

int mainEntry(int argc, char ** argv)
{
    try
    {
        return run(argc, argv);
    }
    catch (...)
    {
        auto error = getCurrentExceptionMessage(true);
        std::cout << error << std::endl;
        return -1;
    }
}
} // namespace DB::PS::CheckpointTool