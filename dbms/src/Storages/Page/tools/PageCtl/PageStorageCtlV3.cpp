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
#include <Interpreters/Context.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/PatternFormatter.h>
#include <Server/CLIService.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/Universal/RaftDataReader.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <common/types.h>

#include <boost/program_options.hpp>
#include <cstdint>
#include <magic_enum.hpp>
#include <unordered_set>

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
        DISPLAY_REGION_INFO = 6,
        DISPLAY_BLOB_DATA = 7,
    };

    std::vector<std::string> paths;
    DisplayType mode = DisplayType::DISPLAY_SUMMARY_INFO;
    UInt64 page_id = UINT64_MAX;
    BlobFileId blob_id = INVALID_BLOBFILE_ID;
    BlobFileOffset blob_offset = INVALID_BLOBFILE_OFFSET;
    size_t blob_size = UINT64_MAX;
    UInt64 namespace_id = DB::TEST_NAMESPACE_ID;
    StorageType storage_type = StorageType::Unknown; // only useful for universal page storage
    UInt32 keyspace_id = NullspaceID; // only useful for universal page storage

    // Show the entries for each Page
    bool show_entries = true;
    // Check the CRC checksum for each fields in pages
    // Only effective when mode == CHECK_ALL_DATA_CRC
    bool check_fields = true;

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
 6 is display all region info
 7 is display blob data (in hex)
)") //
        ("show_entries",
         value<bool>()->default_value(true),
         "Show the entries for each Page. Only effective when `display_mode` is 2") //
        ("check_fields",
         value<bool>()->default_value(true),
         "Also check the checksum for every field inside a Page. Only effective when `display_mode` is 4.") //
        ("storage_type,S", value<int>()->default_value(0), R"(Storage Type(Only useful for UniversalPageStorage):
 1 is Log
 2 is Data
 3 is Meta
 4 is KVStore
)") //
        ("keyspace_id,K", value<UInt32>()->default_value(NullspaceID), "Specify keyspace id.") //
        ("namespace_id,N",
         value<UInt64>()->default_value(DB::TEST_NAMESPACE_ID),
         "When used `page_id`/`blob_id` to query results. You can specify a namespace id.") //
        ("page_id",
         value<UInt64>()->default_value(UINT64_MAX),
         "Query a single Page id, and print its version chain.") //
        ("blob_id,B",
         value<BlobFileId>()->default_value(INVALID_BLOBFILE_ID),
         "Specify the blob_id") //
        ("blob_offset",
         value<BlobFileOffset>()->default_value(INVALID_BLOBFILE_OFFSET),
         "Specify the offset.") //
        ("blob_size",
         value<size_t>()->default_value(0),
         "Specify the size.") //
        //
        ("imitative,I",
         value<bool>()->default_value(true),
         "Use imitative context instead. (encryption is not supported in this mode so that no need to set "
         "config_file_path)") //
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
    opt.blob_id = options["blob_id"].as<BlobFileId>();
    opt.blob_offset = options["blob_offset"].as<BlobFileOffset>();
    opt.blob_size = options["blob_size"].as<size_t>();
    opt.show_entries = options["show_entries"].as<bool>();
    opt.check_fields = options["check_fields"].as<bool>();
    auto storage_type_int = options["storage_type"].as<int>();
    opt.keyspace_id = options["keyspace_id"].as<UInt32>();
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

    if (auto storage_type = magic_enum::enum_cast<StorageType>(storage_type_int); !storage_type)
    {
        std::cerr << "Invalid storage type: " << storage_type_int << std::endl;
        std::cerr << desc << std::endl;
        exit(0);
    }
    else
    {
        opt.storage_type = storage_type.value();
    }

    return opt;
}

template <typename Trait>
class PageStorageControlV3;

namespace u128
{
struct PageStorageControlV3Trait
{
    using PageId = PageIdV3Internal;
    using PageIdTrait = PageIdTrait;
    using PageDirectory = PageDirectoryType;
    using BlobStore = BlobStoreType;
    using PageDirectoryFactory = PageDirectoryFactory;
};
using PageStorageControlV3 = DB::PS::V3::PageStorageControlV3<PageStorageControlV3Trait>;
} // namespace u128
namespace universal
{
struct PageStorageControlV3Trait
{
    using PageId = UniversalPageId;
    using PageIdTrait = PageIdTrait;
    using PageDirectory = PageDirectoryType;
    using BlobStore = BlobStoreType;
    using PageDirectoryFactory = PageDirectoryFactory;
};
using PageStorageControlV3 = DB::PS::V3::PageStorageControlV3<PageStorageControlV3Trait>;
} // namespace universal

template <typename Trait>
class PageStorageControlV3
{
public:
    using PageId = typename Trait::PageId;
    using PageIdAndEntry = std::pair<PageId, PageEntryV3>;
    using PageIdAndEntries = std::vector<PageIdAndEntry>;

public:
    explicit PageStorageControlV3(const ControlOptions & options_)
        : options(options_)
    {}

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
        fmt::print("Running with mode={}\n", magic_enum::enum_name(options.mode));
        if (options.mode == ControlOptions::DisplayType::DISPLAY_WAL_ENTRIES)
        {
            // Only restore the PageDirectory
            typename Trait::PageDirectoryFactory factory;
            factory.debug.dump_entries = true;
            factory.debug.apply_entries_to_directory = false;
            factory.create(String(NAME), provider, delegator, WALConfig::from(config));
            return 0;
        }

        // Other display mode need to restore ps instance
        auto display = [](auto & mvcc_table_directory, auto & blob_store, const ControlOptions & opts) {
            switch (opts.mode)
            {
            case ControlOptions::DisplayType::DISPLAY_SUMMARY_INFO:
            {
                std::cout << getSummaryInfo(mvcc_table_directory, blob_store) << std::endl;
                break;
            }
            case ControlOptions::DisplayType::DISPLAY_DIRECTORY_INFO:
            {
                fmt::print(
                    "{}\n",
                    getDirectoryInfo(
                        mvcc_table_directory,
                        opts.show_entries,
                        opts.storage_type,
                        opts.keyspace_id,
                        opts.namespace_id,
                        opts.page_id));
                break;
            }
            case ControlOptions::DisplayType::DISPLAY_BLOBS_INFO:
            {
                std::cout << getBlobsInfo(blob_store, opts.blob_id) << std::endl;
                break;
            }
            case ControlOptions::DisplayType::CHECK_ALL_DATA_CRC:
            {
                if (opts.page_id != UINT64_MAX)
                {
                    std::cout << checkSinglePage(
                        mvcc_table_directory,
                        blob_store,
                        opts.storage_type,
                        opts.keyspace_id,
                        opts.namespace_id,
                        opts.page_id)
                              << std::endl;
                }
                else
                {
                    std::cout << checkAllDataCrc(mvcc_table_directory, blob_store, opts.check_fields) << std::endl;
                }
                break;
            }
            case ControlOptions::DisplayType::DISPLAY_REGION_INFO:
            {
                if constexpr (std::is_same_v<Trait, universal::PageStorageControlV3Trait>)
                {
                    std::cout << getAllRegionInfo(mvcc_table_directory) << std::endl;
                }
                else
                {
                    std::cout << "Only UniversalPageStorage support this mode." << std::endl;
                }
                break;
            }
            case ControlOptions::DisplayType::DISPLAY_BLOB_DATA:
            {
                String hex_data = getBlobData(blob_store, opts.blob_id, opts.blob_offset, opts.blob_size);
                fmt::print("hex:{}\n", hex_data);
                break;
            }
            default:
                std::cout << "Invalid display mode." << std::endl;
                break;
            }
        };

        if constexpr (std::is_same_v<Trait, u128::PageStorageControlV3Trait>)
        {
            PageStorageImpl ps(String(NAME), delegator, config, provider);
            ps.restore();
            auto & mvcc_table_directory = ps.page_directory->mvcc_table_directory;
            auto & blobstore = ps.blob_store;
            display(mvcc_table_directory, blobstore, options);
        }
        else if constexpr (std::is_same_v<Trait, universal::PageStorageControlV3Trait>)
        {
            auto ps = UniversalPageStorage::create(String(NAME), delegator, config, provider);
            ps->restore();
            auto & mvcc_table_directory = ps->page_directory->mvcc_table_directory;
            auto & blobstore = ps->blob_store;
            display(mvcc_table_directory, *blobstore, options);
        }

        return 0;
    }

    static String getBlobsInfo(typename Trait::BlobStore & blob_store, BlobFileId blob_id)
    {
        auto stat_info = [](const BlobStats::BlobStatPtr & stat, const String & path) {
            FmtBuffer stat_str;
            stat_str.fmtAppend(
                "    stat id: {}\n"
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
                if (blob_id != INVALID_BLOBFILE_ID)
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

        if (blob_id != INVALID_BLOBFILE_ID)
        {
            stats_info.fmtAppend("    no found blob {}", blob_id);
        }
        return stats_info.toString();
    }

    static String getDirectoryInfo(
        typename Trait::PageDirectory::MVCCMapType & mvcc_table_directory,
        bool show_entries,
        StorageType storage_type,
        KeyspaceID keyspace_id,
        UInt64 ns_id,
        UInt64 page_id)
    {
        auto page_info = [show_entries]( //
                             const auto & page_internal_id_,
                             const auto & versioned_entries,
                             const String & extra_msg) {
            FmtBuffer page_str;
            page_str.fmtAppend("    page id {} {}\n", page_internal_id_, extra_msg);
            page_str.fmtAppend("      {}\n", versioned_entries->toDebugString());

            if (!show_entries)
            {
                return page_str.toString();
            }

            size_t count = 0;
            for (const auto & [version, entry_or_del] : versioned_entries->entries)
            {
                if (entry_or_del.isEntry())
                {
                    const auto & entry = entry_or_del.entry.value();
                    page_str.fmtAppend(
                        "      entry {}\n"
                        "       sequence: {}\n"
                        "       epoch: {}\n"
                        "       is del: false\n"
                        "       blob id: {}\n"
                        "       offset: {}\n"
                        "       size: {}\n"
                        "       crc: 0x{:X}\n", //
                        count++, //
                        version.sequence, //
                        version.epoch, //
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
                else
                {
                    page_str.append("      entry is null\n"
                                    "       is del: true\n");
                }
            }
            return page_str.toString();
        };

        FmtBuffer directory_info;
        directory_info.append("  Directory specific info: \n\n");
        for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
        {
            // Show all page_id
            if (page_id == UINT64_MAX)
            {
                String extra_msg;
                if constexpr (std::is_same_v<Trait, universal::PageStorageControlV3Trait>)
                {
                    if (auto maybe_region_id = RaftDataReader::tryParseRegionId(internal_id); maybe_region_id)
                        extra_msg = fmt::format("(region_id={})", *maybe_region_id);
                }
                directory_info.append(page_info(internal_id, versioned_entries, extra_msg));
                continue;
            }

            // Only show the given page_id
            if constexpr (std::is_same_v<Trait, u128::PageStorageControlV3Trait>)
            {
                if (internal_id.low == page_id && internal_id.high == ns_id)
                {
                    directory_info.append(page_info(internal_id, versioned_entries, ""));
                    return directory_info.toString();
                }
            }
            else if constexpr (std::is_same_v<Trait, universal::PageStorageControlV3Trait>)
            {
                RUNTIME_CHECK_MSG(
                    storage_type == StorageType::Log || storage_type == StorageType::Data
                        || storage_type == StorageType::Meta || storage_type == StorageType::KVStore,
                    "Unsupported storage type"); // NOLINT(readability-simplify-boolean-expr)
                auto prefix = UniversalPageIdFormat::toFullPrefix(keyspace_id, storage_type, ns_id);
                auto full_page_id = UniversalPageIdFormat::toFullPageId(prefix, page_id);
                if (full_page_id == internal_id)
                {
                    directory_info.append(page_info(internal_id, versioned_entries, ""));
                    return directory_info.toString();
                }
            }
        }

        if (page_id != UINT64_MAX)
        {
            directory_info.fmtAppend("    no found page {}", page_id);
        }
        return directory_info.toString();
    }

    struct RegionSummary
    {
        RegionID region_id;
        UInt64 min_raft_log_index;
        UInt64 max_raft_log_index;
        UInt64 num_raft_log;
    };

    static String getAllRegionInfo(universal::PageDirectoryType::MVCCMapType & mvcc_table_directory)
    {
        // region_id -> pair<min_raft_log_index, max_raft_log_index>
        size_t tot_num_raft_log = 0;
        std::unordered_map<RegionID, RegionSummary> regions;
        for (const auto & [page_id, _] : mvcc_table_directory)
        {
            auto maybe_region_id = RaftDataReader::tryParseRegionId(page_id);
            if (!maybe_region_id)
                continue;

            auto region_id = *maybe_region_id;
            regions.try_emplace(
                region_id,
                RegionSummary{
                    .region_id = region_id,
                    .min_raft_log_index = UINT64_MAX,
                    .max_raft_log_index = 0,
                    .num_raft_log = 0});

            auto maybe_raft_log_index = RaftDataReader::tryParseRaftLogIndex(page_id);
            if (!maybe_raft_log_index)
                continue;

            auto raft_log_index = *maybe_raft_log_index;
            auto & summary = regions[region_id];
            summary.min_raft_log_index = std::min(summary.min_raft_log_index, raft_log_index);
            summary.max_raft_log_index = std::max(summary.max_raft_log_index, raft_log_index);
            summary.num_raft_log += 1;

            tot_num_raft_log += 1;
        }

        std::vector<RegionSummary> region_infos_vec;
        region_infos_vec.reserve(regions.size());
        for (const auto & [region_id, summary] : regions)
            region_infos_vec.emplace_back(summary);
        // sort by raft log count
        sort(region_infos_vec.begin(), region_infos_vec.end(), [](const auto & lhs, const auto & rhs) {
            return lhs.num_raft_log > rhs.num_raft_log;
        });

        FmtBuffer all_region_info;
        all_region_info.append("  All regions: \n\n");
        all_region_info.fmtAppend(
            "  num_pages={} num_regions={} num_raft_log={}\n",
            mvcc_table_directory.size(),
            region_infos_vec.size(),
            tot_num_raft_log);
        all_region_info.joinStr(
            region_infos_vec.begin(),
            region_infos_vec.end(),
            [](const RegionSummary & summary, FmtBuffer & fb) {
                bool has_log_hole
                    = summary.num_raft_log != (summary.max_raft_log_index - summary.min_raft_log_index + 1);
                fb.fmtAppend(
                    "   region_id={} min_raft_log_index={} max_raft_log_index={} log_count={} has_log_hole={}\n",
                    summary.region_id,
                    summary.min_raft_log_index,
                    summary.max_raft_log_index,
                    summary.num_raft_log,
                    has_log_hole);
            },
            "");

        return all_region_info.toString();
    }

    static String getSummaryInfo(
        typename Trait::PageDirectory::MVCCMapType & mvcc_table_directory,
        typename Trait::BlobStore & blob_store)
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

        dir_summary_info.fmtAppend(
            "    total pages: {}, longest version chain: {}, shortest version chain: {}\n\n",
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
                    fb.fmtAppend(
                        "   stat id: {}\n"
                        "     path: {}\n"
                        "     total size: {}\n"
                        "     valid size: {}\n"
                        "     valid rate: {:.2f}%\n"
                        "     max cap: {}\n",
                        stat->id,
                        arg.first,
                        stat->sm_total_size,
                        stat->sm_valid_size,
                        stat->sm_valid_rate * 100,
                        stat->sm_max_caps);
                }
            },
            "");

        return dir_summary_info.toString();
    }

    static String checkSinglePage(
        typename Trait::PageDirectory::MVCCMapType & mvcc_table_directory,
        typename Trait::BlobStore & blob_store,
        StorageType storage_type,
        KeyspaceID keyspace_id,
        UInt64 ns_id,
        UInt64 page_id)
    {
        auto check = [&](auto & full_page_id) {
            const auto & it = mvcc_table_directory.find(full_page_id);
            if (it == mvcc_table_directory.end())
            {
                return fmt::format("Can't find {}", full_page_id);
            }

            FmtBuffer error_msg;
            size_t error_count = 0;
            size_t ignore_count = 0;
            for (const auto & [version, entry_or_del] : it->second->entries)
            {
                if (entry_or_del.isEntry() && it->second->type == EditRecordType::VAR_ENTRY)
                {
                    const PageEntryV3 & entry = entry_or_del.entry.value();
                    if (entry.checkpoint_info.has_value() && entry.checkpoint_info.is_local_data_reclaimed)
                    {
                        error_msg.fmtAppend("  page {} version {} local data is reclaimed\n", full_page_id, version);
                        ++ignore_count;
                        continue;
                    }
                    try
                    {
                        PageIdAndEntry to_read_entry;
                        PageIdAndEntries to_read;
                        to_read_entry.first = full_page_id;
                        to_read_entry.second = entry;

                        to_read.emplace_back(to_read_entry);
                        blob_store.read(to_read);

                        if (!entry.field_offsets.empty())
                        {
                            DB::PageStorage::FieldIndices indices(entry.field_offsets.size());
                            std::iota(std::begin(indices), std::end(indices), 0);

                            typename Trait::BlobStore::FieldReadInfos infos;
                            typename Trait::BlobStore::FieldReadInfo info(full_page_id, entry, indices);
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

            if (error_count == 0 && ignore_count == 0)
            {
                return fmt::format("Checked {} without any error.", full_page_id);
            }

            error_msg.fmtAppend("Check {} meet {} errors {} ignores!", full_page_id, error_count, ignore_count);
            return error_msg.toString();
        };

        if constexpr (std::is_same_v<Trait, u128::PageStorageControlV3Trait>)
        {
            const auto & page_internal_id = buildV3Id(ns_id, page_id);
            return check(page_internal_id);
        }
        else if constexpr (std::is_same_v<Trait, universal::PageStorageControlV3Trait>)
        {
            RUNTIME_CHECK_MSG(
                storage_type == StorageType::Log || storage_type == StorageType::Data
                    || storage_type == StorageType::Meta || storage_type == StorageType::KVStore,
                "Unsupported storage type"); // NOLINT(readability-simplify-boolean-expr)
            auto prefix = UniversalPageIdFormat::toFullPrefix(keyspace_id, storage_type, ns_id);
            auto full_page_id = UniversalPageIdFormat::toFullPageId(prefix, page_id);
            return check(full_page_id);
        }
        __builtin_unreachable();
    }

    static String checkAllDataCrc(
        typename Trait::PageDirectory::MVCCMapType & mvcc_table_directory,
        typename Trait::BlobStore & blob_store,
        bool check_fields)
    {
        size_t total_pages = mvcc_table_directory.size();
        size_t cut_index = 0;
        size_t index = 0;
        fmt::print("Begin to check CRC for all pages. check_fields={}\n", check_fields);

        std::list<std::pair<typename Trait::PageId, PageVersion>> error_versioned_pages;
        for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
        {
            if (index == total_pages / 10 * cut_index)
            {
                fmt::print("processing : {}%\n", cut_index * 10);
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
                        PageIdAndEntry to_read_entry;
                        const PageEntryV3 & entry = entry_or_del.entry.value();
                        PageIdAndEntries to_read;
                        to_read_entry.first = internal_id;
                        to_read_entry.second = entry;

                        to_read.emplace_back(to_read_entry);
                        blob_store.read(to_read);

                        if (check_fields && !entry.field_offsets.empty())
                        {
                            DB::PageStorage::FieldIndices indices(entry.field_offsets.size());
                            std::iota(std::begin(indices), std::end(indices), 0);

                            typename Trait::BlobStore::FieldReadInfos infos;
                            typename Trait::BlobStore::FieldReadInfo info(internal_id, entry, indices);
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

    static String getBlobData(
        typename Trait::BlobStore & blob_store,
        BlobFileId blob_id,
        BlobFileOffset offset,
        size_t size)
    {
        auto page_id = []() {
            if constexpr (std::is_same_v<Trait, u128::PageStorageControlV3Trait>)
                return PageIdV3Internal(0, 0);
            else
                return UniversalPageId("");
        }();
        char * buffer = new char[size];
        blob_store.read(page_id, blob_id, offset, buffer, size, nullptr, false);

        using ChecksumClass = Digest::CRC64;
        ChecksumClass digest;
        digest.update(buffer, size);
        auto checksum = digest.checksum();
        fmt::print("checksum: 0x{:X}\n", checksum);

        auto hex_str = Redact::keyToHexString(buffer, size);
        delete[] buffer;
        return hex_str;
    }

private:
    ControlOptions options;
};

template class PageStorageControlV3<u128::PageStorageControlV3Trait>;
template class PageStorageControlV3<universal::PageStorageControlV3Trait>;
} // namespace DB::PS::V3

using namespace DB::PS::V3;

void pageStorageV3CtlEntry(int argc, char ** argv)
{
    bool enable_colors = isatty(STDERR_FILENO) && isatty(STDOUT_FILENO);
    DB::tests::TiFlashTestEnv::setupLogger("trace", std::cerr, enable_colors);
    const auto & options = ControlOptions::parse(argc, argv);
    u128::PageStorageControlV3(options).run();
}

void universalpageStorageCtlEntry(int argc, char ** argv)
{
    bool enable_colors = isatty(STDERR_FILENO) && isatty(STDOUT_FILENO);
    DB::tests::TiFlashTestEnv::setupLogger("trace", std::cerr, enable_colors);
    const auto & options = ControlOptions::parse(argc, argv);
    universal::PageStorageControlV3(options).run();
}
