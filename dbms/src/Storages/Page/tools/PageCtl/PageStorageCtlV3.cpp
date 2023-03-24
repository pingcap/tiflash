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

#include <Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/PatternFormatter.h>
#include <Server/CLIService.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashTestEnv.h>

#include <boost/program_options.hpp>
#include <magic_enum.hpp>

namespace DB::PS::V3
{
extern "C" {
void run_raftstore_proxy_ffi(int argc, const char * const * argv, const DB::EngineStoreServerHelper *);
}
struct ControlOptions
{
    enum class DisplayType
    {
        DISPLAY_SUMMARY_INFO = 1,
        DISPLAY_DIRECTORY_INFO = 2,
        DISPLAY_BLOBS_INFO = 3,
        CHECK_ALL_DATA_CRC = 4,
        DISPLAY_WAL_ENTRIES = 5,
    };

    std::vector<std::string> paths;
    DisplayType mode = DisplayType::DISPLAY_SUMMARY_INFO;
    UInt64 page_id = UINT64_MAX;
    UInt32 blob_id = UINT32_MAX;
    UInt64 namespace_id = DB::TEST_NAMESPACE_ID;
    bool enable_fo_check = true;
    bool is_imitative = true;
    String config_file_path;

    static ControlOptions parse(int argc, char ** argv);
};


ControlOptions ControlOptions::parse(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using po::value;

    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "produce help message") //
        ("paths,P", value<std::vector<std::string>>(), "store path(s)") //
        ("mode", value<int>()->default_value(1), R"(Display Mode:
 1 is summary information
 2 is display all of stored page and version chain(will be very long)
 3 is display all blobs(in disk) data distribution
 4 is check every data is valid
 5 is dump entries in WAL log files
)") //
        ("enable_fo_check,E", value<bool>()->default_value(true), "Also check the evert field offsets. This options only works when `display_mode` is 4.") //
        ("namespace_id,N", value<UInt64>()->default_value(DB::TEST_NAMESPACE_ID), "When used `page_id`/`blob_id` to query results. You can specify a namespace id.") //
        ("page_id", value<UInt64>()->default_value(UINT64_MAX), "Query a single Page id, and print its version chain.") //
        ("blob_id,B", value<UInt32>()->default_value(UINT32_MAX), "Query a single Blob id, and print its data distribution.") //
        ("imitative,I", value<bool>()->default_value(true), "Use imitative context instead. (encryption is not supported in this mode so that no need to set config_file_path)") //
        ("config_file_path", value<std::string>(), "Path to TiFlash config (tiflash.toml).");


    static_assert(sizeof(DB::PageIdU64) == sizeof(UInt64));
    static_assert(sizeof(DB::BlobFileId) == sizeof(UInt64));

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);
    po::notify(options);

    if (options.count("help") > 0)
    {
        std::cerr << desc << std::endl;
        exit(0);
    }

    ControlOptions opt;

    if (options.count("paths") == 0)
    {
        std::cerr << "Invalid arg paths." << std::endl;
        std::cerr << desc << std::endl;
        exit(0);
    }
    opt.paths = options["paths"].as<std::vector<std::string>>();
    auto mode_int = options["mode"].as<int>();
    opt.page_id = options["page_id"].as<UInt64>();
    opt.blob_id = options["blob_id"].as<UInt32>();
    opt.enable_fo_check = options["enable_fo_check"].as<bool>();
    opt.namespace_id = options["namespace_id"].as<UInt64>();
    opt.is_imitative = options["imitative"].as<bool>();
    if (opt.is_imitative && options.count("config_file_path") != 0)
    {
        std::cerr << "config_file_path is not allowed in imitative mode" << std::endl;
        exit(0);
    }
    else if (!opt.is_imitative && options.count("config_file_path") == 0)
    {
        std::cerr << "config_file_path is required in proxy mode" << std::endl;
        exit(0);
    }
    if (options.count("config_file_path") != 0)
    {
        opt.config_file_path = options["config_file_path"].as<std::string>();
    }

    if (auto mode = magic_enum::enum_cast<DisplayType>(mode_int); !mode)
    {
        std::cerr << "Invalid display mode: " << mode_int << std::endl;
        std::cerr << desc << std::endl;
        exit(0);
    }
    else
    {
        opt.mode = mode.value();
    }

    return opt;
}

class PageStorageControlV3
{
public:
    explicit PageStorageControlV3(const ControlOptions & options_)
        : options(options_)
    {
    }

    void run()
    {
        try
        {
            if (options.is_imitative)
            {
                auto context = Context::createGlobal();
                getPageStorageV3Info(*context, options);
            }
            else
            {
                CLIService service(getPageStorageV3Info, options, options.config_file_path, run_raftstore_proxy_ffi);
                service.run({""});
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException("exception thrown");
            std::abort(); // Finish testing if some error happened.
        }
    }

private:
    static int getPageStorageV3Info(Context & context, const ControlOptions & options)
    {
        DB::PSDiskDelegatorPtr delegator;
        if (options.paths.size() == 1)
        {
            delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0]);
        }
        else
        {
            delegator = std::make_shared<DB::tests::MockDiskDelegatorMulti>(options.paths);
        }

        FileProviderPtr provider;
        if (options.is_imitative)
        {
            auto key_manager = std::make_shared<DB::MockKeyManager>(false);
            provider = std::make_shared<DB::FileProvider>(key_manager, false);
        }
        else
        {
            provider = context.getFileProvider();
        }

        constexpr static std::string_view NAME = "PageStorageControlV3";
        PageStorageConfig config;
        if (options.mode == ControlOptions::DisplayType::DISPLAY_WAL_ENTRIES)
        {
            // Only restore the PageDirectory
            PageDirectoryFactory<u128::FactoryTrait> factory;
            factory.dump_entries = true;
            factory.create(String(NAME), provider, delegator, WALConfig::from(config));
            return 0;
        }

        // Other display mode need to restore ps instance
        PageStorageImpl ps(String(NAME), delegator, config, provider);
        ps.restore();
        PageDirectory<u128::PageDirectoryTrait>::MVCCMapType & mvcc_table_directory = ps.page_directory->mvcc_table_directory;

        switch (options.mode)
        {
        case ControlOptions::DisplayType::DISPLAY_SUMMARY_INFO:
        {
            std::cout << getSummaryInfo(mvcc_table_directory, ps.blob_store) << std::endl;
            break;
        }
        case ControlOptions::DisplayType::DISPLAY_DIRECTORY_INFO:
        {
            std::cout << getDirectoryInfo(mvcc_table_directory, options.namespace_id, options.page_id) << std::endl;
            break;
        }
        case ControlOptions::DisplayType::DISPLAY_BLOBS_INFO:
        {
            std::cout << getBlobsInfo(ps.blob_store, options.blob_id) << std::endl;
            break;
        }
        case ControlOptions::DisplayType::CHECK_ALL_DATA_CRC:
        {
            if (options.page_id != UINT64_MAX)
            {
                std::cout << checkSinglePage(mvcc_table_directory, ps.blob_store, options.namespace_id, options.page_id) << std::endl;
            }
            else
            {
                std::cout << checkAllDataCrc(mvcc_table_directory, ps.blob_store, options.enable_fo_check) << std::endl;
            }
            break;
        }
        default:
            std::cout << "Invalid display mode." << std::endl;
            break;
        }
        return 0;
    }

    static String getBlobsInfo(BlobStore<u128::BlobStoreTrait> & blob_store, UInt32 blob_id)
    {
        auto stat_info = [](const BlobStats::BlobStatPtr & stat, const String & path) {
            FmtBuffer stat_str;
            stat_str.fmtAppend("    stat id: {}\n"
                               "     path: {}\n"
                               "     total size: {}\n"
                               "     valid size: {}\n"
                               "     valid rate: {}\n"
                               "     max cap: {}\n", //
                               stat->id, //
                               path,
                               stat->sm_total_size, //
                               stat->sm_valid_size, //
                               stat->sm_valid_rate, //
                               stat->sm_max_caps);

            stat_str.append(stat->smap->toDebugString());
            stat_str.append("\n");
            return stat_str.toString();
        };

        FmtBuffer stats_info;
        stats_info.append("  Blobs specific info: \n\n");

        for (const auto & [path, stats] : blob_store.blob_stats.getStats())
        {
            for (const auto & stat : stats)
            {
                if (blob_id != UINT32_MAX)
                {
                    if (stat->id == blob_id)
                    {
                        stats_info.append(stat_info(stat, path));
                        return stats_info.toString();
                    }
                    continue;
                }

                stats_info.append(stat_info(stat, path));
            }
        }

        if (blob_id != UINT32_MAX)
        {
            stats_info.fmtAppend("    no found blob {}", blob_id);
        }
        return stats_info.toString();
    }

    static String getDirectoryInfo(PageDirectory<u128::PageDirectoryTrait>::MVCCMapType & mvcc_table_directory, UInt64 ns_id, UInt64 page_id)
    {
        auto page_info = [](UInt128 page_internal_id_, const u128::VersionedPageEntriesPtr & versioned_entries) {
            FmtBuffer page_str;
            page_str.fmtAppend("    page id {}\n", page_internal_id_);
            page_str.fmtAppend("      {}\n", versioned_entries->toDebugString());

            size_t count = 0;
            for (const auto & [version, entry_or_del] : versioned_entries->entries)
            {
                const auto & entry = entry_or_del.entry;
                page_str.fmtAppend("      entry {}\n"
                                   "       sequence: {}\n"
                                   "       epoch: {}\n"
                                   "       is del: {}\n"
                                   "       blob id: {}\n"
                                   "       offset: {}\n"
                                   "       size: {}\n"
                                   "       crc: {}\n", //
                                   count++, //
                                   version.sequence, //
                                   version.epoch, //
                                   entry_or_del.isDelete(), //
                                   entry.file_id, //
                                   entry.offset, //
                                   entry.size, //
                                   entry.checksum, //
                                   entry.field_offsets.size() //
                );
                if (!entry.field_offsets.empty())
                {
                    page_str.append("          field offset:\n");
                    for (const auto & [offset, crc] : entry.field_offsets)
                    {
                        page_str.fmtAppend("            offset: {} crc: 0x{:X}\n", offset, crc);
                    }
                    page_str.append("\n");
                }
            }
            return page_str.toString();
        };

        FmtBuffer directory_info;
        directory_info.append("  Directory specific info: \n\n");
        for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
        {
            if (page_id != UINT64_MAX)
            {
                if (internal_id.low == page_id && internal_id.high == ns_id)
                {
                    directory_info.append(page_info(internal_id, versioned_entries));
                    return directory_info.toString();
                }
                continue;
            }
            directory_info.append(page_info(internal_id, versioned_entries));
        }

        if (page_id != UINT64_MAX)
        {
            directory_info.fmtAppend("    no found page {}", page_id);
        }
        return directory_info.toString();
    }

    static String getSummaryInfo(PageDirectory<u128::PageDirectoryTrait>::MVCCMapType & mvcc_table_directory, BlobStore<u128::BlobStoreTrait> & blob_store)
    {
        UInt64 longest_version_chaim = 0;
        UInt64 shortest_version_chaim = UINT64_MAX;
        FmtBuffer dir_summary_info;

        dir_summary_info.append("  Directory summary info: \n");

        for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
        {
            (void)internal_id;
            longest_version_chaim = std::max(longest_version_chaim, versioned_entries->size());
            shortest_version_chaim = std::min(shortest_version_chaim, versioned_entries->size());
        }

        dir_summary_info.fmtAppend("    total pages: {}, longest version chaim: {} , shortest version chaim: {} \n\n",
                                   mvcc_table_directory.size(),
                                   longest_version_chaim,
                                   shortest_version_chaim);

        dir_summary_info.append("  Blobs summary info: \n");
        const auto & blob_stats = blob_store.blob_stats.getStats();
        dir_summary_info.joinStr(
            blob_stats.begin(),
            blob_stats.end(),
            [](const auto arg, FmtBuffer & fb) {
                for (const auto & stat : arg.second)
                {
                    fb.fmtAppend("   stat id: {}\n"
                                 "     path: {}\n"
                                 "     total size: {}\n"
                                 "     valid size: {}\n"
                                 "     valid rate: {}\n"
                                 "     max cap: {}\n",
                                 stat->id,
                                 arg.first,
                                 stat->sm_total_size,
                                 stat->sm_valid_size,
                                 stat->sm_valid_rate,
                                 stat->sm_max_caps);
                }
            },
            "");

        return dir_summary_info.toString();
    }

    static String checkSinglePage(PageDirectory<u128::PageDirectoryTrait>::MVCCMapType & mvcc_table_directory, BlobStore<u128::BlobStoreTrait> & blob_store, UInt64 ns_id, UInt64 page_id)
    {
        const auto & page_internal_id = buildV3Id(ns_id, page_id);
        const auto & it = mvcc_table_directory.find(page_internal_id);
        if (it == mvcc_table_directory.end())
        {
            return fmt::format("Can't find {}", page_internal_id);
        }

        FmtBuffer error_msg;
        size_t error_count = 0;
        for (const auto & [version, entry_or_del] : it->second->entries)
        {
            if (entry_or_del.isEntry() && it->second->type == EditRecordType::VAR_ENTRY)
            {
                (void)blob_store;
                try
                {
                    PageIDAndEntryV3 to_read_entry;
                    const PageEntryV3 & entry = entry_or_del.entry;
                    PageIDAndEntriesV3 to_read;
                    to_read_entry.first = page_internal_id;
                    to_read_entry.second = entry;

                    to_read.emplace_back(to_read_entry);
                    blob_store.read(to_read);

                    if (!entry.field_offsets.empty())
                    {
                        DB::PageStorage::FieldIndices indices(entry.field_offsets.size());
                        std::iota(std::begin(indices), std::end(indices), 0);

                        BlobStore<u128::BlobStoreTrait>::FieldReadInfos infos;
                        BlobStore<u128::BlobStoreTrait>::FieldReadInfo info(page_internal_id, entry, indices);
                        infos.emplace_back(info);
                        blob_store.read(infos);
                    }
                }
                catch (DB::Exception & e)
                {
                    error_count++;
                    error_msg.append(e.displayText());
                    error_msg.append("\n");
                }
            }
        }

        if (error_count == 0)
        {
            return fmt::format("Checked {} without any error.", page_internal_id);
        }

        error_msg.fmtAppend("Check {} meet {} errors!", page_internal_id, error_count);
        return error_msg.toString();
    }

    static String checkAllDataCrc(PageDirectory<u128::PageDirectoryTrait>::MVCCMapType & mvcc_table_directory, BlobStore<u128::BlobStoreTrait> & blob_store, bool enable_fo_check)
    {
        size_t total_pages = mvcc_table_directory.size();
        size_t cut_index = 0;
        size_t index = 0;
        std::cout << fmt::format("Begin to check all of datas CRC. enable_fo_check={}", static_cast<int>(enable_fo_check)) << std::endl;

        std::list<std::pair<UInt128, PageVersion>> error_versioned_pages;
        for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
        {
            if (index == total_pages / 10 * cut_index)
            {
                std::cout << fmt::format("processing : {}%", cut_index * 10) << std::endl;
                cut_index++;
            }

            // TODO : need replace by getLastEntry();
            for (const auto & [version, entry_or_del] : versioned_entries->entries)
            {
                if (entry_or_del.isEntry() && versioned_entries->type == EditRecordType::VAR_ENTRY)
                {
                    (void)blob_store;
                    try
                    {
                        PageIDAndEntryV3 to_read_entry;
                        const PageEntryV3 & entry = entry_or_del.entry;
                        PageIDAndEntriesV3 to_read;
                        to_read_entry.first = internal_id;
                        to_read_entry.second = entry;

                        to_read.emplace_back(to_read_entry);
                        blob_store.read(to_read);

                        if (enable_fo_check && !entry.field_offsets.empty())
                        {
                            DB::PageStorage::FieldIndices indices(entry.field_offsets.size());
                            std::iota(std::begin(indices), std::end(indices), 0);

                            BlobStore<u128::BlobStoreTrait>::FieldReadInfos infos;
                            BlobStore<u128::BlobStoreTrait>::FieldReadInfo info(internal_id, entry, indices);
                            infos.emplace_back(info);
                            blob_store.read(infos);
                        }
                    }
                    catch (DB::Exception & e)
                    {
                        error_versioned_pages.emplace_back(std::make_pair(internal_id, version));
                    }
                }
            }
            index++;
        }

        if (error_versioned_pages.empty())
        {
            return "All of data checked. All passed.";
        }

        FmtBuffer error_msg;
        error_msg.append("Found error in these pages: ");
        for (const auto & [internal_id, versioned] : error_versioned_pages)
        {
            error_msg.fmtAppend("id: {}, sequence: {}, epoch: {} \n", internal_id, versioned.sequence, versioned.epoch);
        }
        error_msg.append("Please use `--query_table_id` + `--page_id` to get the more error info.");

        return error_msg.toString();
    }

private:
    ControlOptions options;
};


} // namespace DB::PS::V3

using namespace DB::PS::V3;

void pageStorageV3CtlEntry(int argc, char ** argv)
{
    DB::tests::TiFlashTestEnv::setupLogger();
    const auto & options = ControlOptions::parse(argc, argv);
    PageStorageControlV3(options).run();
}
