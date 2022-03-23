#include <Common/UnifiedLogPatternFormatter.h>
#include <Encryption/MockKeyManager.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>

#include <boost/program_options.hpp>

namespace DB::PS::V3
{
struct ControlOptions
{
    enum DisplayType
    {
        DISPLAY_SUMMARY_INFO = 1,
        DISPLAY_DIRECTORY_INFO = 2,
        DISPLAY_BLOBS_INFO = 3,
        CHECK_ALL_DATA_CRC = 4,
    };

    std::vector<std::string> paths;
    int display_mode = DisplayType::DISPLAY_SUMMARY_INFO;
    UInt64 query_page_id = UINT64_MAX;
    UInt32 query_blob_id = UINT32_MAX;
    UInt64 query_ns_id = DB::TEST_NAMESPACE_ID;
    UInt64 check_page_id = UINT64_MAX;
    bool enable_fo_check = true;

    static ControlOptions parse(int argc, char ** argv);
};


ControlOptions ControlOptions::parse(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using po::value;

    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "produce help message") //
        ("paths,P", value<std::vector<std::string>>(), "store path(s)") //
        ("display_mode,D", value<int>()->default_value(1), "Display Mode: 1 is summary information,\n 2 is display all of stored page and version chaim(will be very long),\n 3 is display all blobs(in disk) data distribution. \n 4 is check every data is valid.") //
        ("enable_fo_check,E", value<bool>()->default_value(true), "Also check the evert field offsets. This options only works when `display_mode` is 4.") //
        ("query_ns_id,N", value<UInt64>()->default_value(DB::TEST_NAMESPACE_ID), "When used `check_page_id`/`query_page_id`/`query_blob_id` to query results. You can specify a namespace id.")("check_page_id,C", value<UInt64>()->default_value(UINT64_MAX), "Check a single Page id, display the exception if meet. And also will check the field offsets.") //
        ("query_page_id,W", value<UInt64>()->default_value(UINT64_MAX), "Quert a single Page id, and print its version chaim.") //
        ("query_blob_id,B", value<UInt32>()->default_value(UINT32_MAX), "Quert a single Blob id, and print its data distribution.");


    static_assert(sizeof(DB::PageId) == sizeof(UInt64));
    static_assert(sizeof(DB::BlobFileId) == sizeof(UInt32));

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
    opt.display_mode = options["display_mode"].as<int>();
    opt.query_page_id = options["query_page_id"].as<UInt64>();
    opt.query_blob_id = options["query_blob_id"].as<UInt32>();
    opt.enable_fo_check = options["enable_fo_check"].as<bool>();
    opt.check_page_id = options["check_page_id"].as<UInt64>();
    opt.query_ns_id = options["query_ns_id"].as<UInt64>();

    if (opt.display_mode < DisplayType::DISPLAY_SUMMARY_INFO || opt.display_mode > DisplayType::CHECK_ALL_DATA_CRC)
    {
        std::cerr << "Invalid display mode: " << opt.display_mode << std::endl;
        std::cerr << desc << std::endl;
        exit(0);
    }

    return opt;
}

class PageStorageControl
{
public:
    PageStorageControl(const ControlOptions & options_)
        : options(options_)
    {
    }

    void run()
    {
        DB::PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0]);

        auto key_manager = std::make_shared<DB::MockKeyManager>(false);
        auto file_provider = std::make_shared<DB::FileProvider>(key_manager, false);

        BlobStore::Config blob_config;

        // TODO: Need use multi-path after BlobStore supported.
        PageStorage::Config config;
        PageStorageImpl ps_v3("PageStorageControl", delegator, config, file_provider);
        PageDirectory::MVCCMapType & mvcc_table_directory = ps_v3.page_directory->mvcc_table_directory;

        switch (options.display_mode)
        {
        case ControlOptions::DisplayType::DISPLAY_SUMMARY_INFO:
        {
            std::cout << getSummaryInfo(mvcc_table_directory, ps_v3.blob_store) << std::endl;
            break;
        }
        case ControlOptions::DisplayType::DISPLAY_DIRECTORY_INFO:
        {
            std::cout << getDirectoryInfo(mvcc_table_directory, options.query_ns_id, options.query_page_id) << std::endl;
            break;
        }
        case ControlOptions::DisplayType::DISPLAY_BLOBS_INFO:
        {
            std::cout << getBlobsInfo(ps_v3.blob_store, options.query_blob_id) << std::endl;
            break;
        }
        case ControlOptions::DisplayType::CHECK_ALL_DATA_CRC:
        {
            if (options.check_page_id != UINT64_MAX)
            {
                std::cout << checkSinglePage(mvcc_table_directory, ps_v3.blob_store, options.query_ns_id, options.check_page_id) << std::endl;
            }
            else
            {
                std::cout << checkAllDatasCrc(mvcc_table_directory, ps_v3.blob_store, options.enable_fo_check) << std::endl;
            }
            break;
        }
        default:
            std::cout << "Invalid display mode." << std::endl;
            break;
        }
    }

private:
    String getBlobsInfo(BlobStore & blob_store, UInt32 blob_id)
    {
        auto statInfo = [](const BlobStore::BlobStats::BlobStatPtr & stat) {
            String stat_str = fmt::format(R"raw(    stat id: {}
            total size: {}
            valid size: {}
            valid rate: {}
            max cap: {}
                )raw", //
                                          stat->id, //
                                          stat->sm_total_size, //
                                          stat->sm_valid_size, //
                                          stat->sm_valid_rate, //
                                          stat->sm_max_caps);

            stat_str += stat->smap->toDebugString();
            stat_str += "\n";
            return stat_str;
        };

        String stats_str = "  Blobs specific info: \n\n";
        for (const auto & stat : blob_store.blob_stats.getStats())
        {
            if (blob_id != UINT32_MAX)
            {
                if (stat->id == blob_id)
                {
                    stats_str += statInfo(stat);
                    return stats_str;
                }
                continue;
            }
            stats_str += statInfo(stat);
        }

        if (blob_id != UINT32_MAX)
        {
            stats_str += fmt::format("    no found blob {}", blob_id);
        }
        return stats_str;
    }

    String getDirectoryInfo(PageDirectory::MVCCMapType & mvcc_table_directory, UInt64 ns_id, UInt64 page_id)
    {
        auto pageInfo = [](UInt128 page_internal_id_, const VersionedPageEntriesPtr & versioned_entries) {
            String page_str = fmt::format("    page id {}\n", page_internal_id_);
            page_str += fmt::format("      {}\n", versioned_entries->toDebugString());

            size_t count = 0;
            for (const auto & [version, entry_or_del] : versioned_entries->entries)
            {
                const auto & entry = entry_or_del.entry;
                page_str += fmt::format(R"raw(      entry {}
                sequence: {}
                epoch: {}
                is del: {}
                blob id: {}
                offset: {}
                size: {}
                crc: {})raw", //
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
                if (entry.field_offsets.size() != 0)
                {
                    page_str += fmt::format("          field offset:\n");
                    for (const auto & [offset, crc] : entry.field_offsets)
                    {
                        page_str += fmt::format("            offset: {} crc: 0x{:X}\n", offset, crc);
                    }
                    page_str += fmt::format("\n");
                }
            }
            return page_str;
        };

        String directory_str = "  Directory specific info: \n\n";
        for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
        {
            if (page_id != UINT64_MAX)
            {
                if (internal_id.low == page_id && internal_id.high == ns_id)
                {
                    directory_str += pageInfo(internal_id, versioned_entries);
                    return directory_str;
                }
                continue;
            }
            directory_str += pageInfo(internal_id, versioned_entries);
        }

        if (page_id != UINT64_MAX)
        {
            directory_str += fmt::format("    no found page {}", page_id);
        }
        return directory_str;
    }

    String getSummaryInfo(PageDirectory::MVCCMapType & mvcc_table_directory, BlobStore & blob_store)
    {
        UInt64 longest_version_chaim = 0;
        UInt64 shortest_version_chaim = UINT64_MAX;

        String dir_summary_info = "  Directory summary info: \n";

        for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
        {
            (void)internal_id;
            longest_version_chaim = std::max(longest_version_chaim, versioned_entries->size());
            shortest_version_chaim = std::min(shortest_version_chaim, versioned_entries->size());
        }

        dir_summary_info += fmt::format("    total pages: {}, longest version chaim: {} , shortest version chaim: {} \n\n",
                                        mvcc_table_directory.size(),
                                        longest_version_chaim,
                                        shortest_version_chaim);

        String stats_str = "  Blobs summary info: \n";
        for (const auto & stat : blob_store.blob_stats.getStats())
        {
            stats_str += fmt::format(R"raw(    stat id: {}
            total size: {}
            valid size: {}
            valid rate: {}
            max cap: {}
                )raw", //
                                     stat->id,
                                     stat->sm_total_size,
                                     stat->sm_valid_size,
                                     stat->sm_valid_rate,
                                     stat->sm_max_caps);
        }

        return dir_summary_info + stats_str;
    }

    String checkSinglePage(PageDirectory::MVCCMapType & mvcc_table_directory, BlobStore & blob_store, UInt64 ns_id, UInt64 page_id)
    {
        const auto & page_internal_id = buildV3Id(ns_id, page_id);
        const auto & it = mvcc_table_directory.find(page_internal_id);
        if (it == mvcc_table_directory.end())
        {
            return fmt::format("Can't find {}", page_internal_id);
        }

        String error_msg = "";
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

                    if (entry.field_offsets.size() != 0)
                    {
                        DB::PageStorage::FieldIndices indices(entry.field_offsets.size());
                        std::iota(std::begin(indices), std::end(indices), 0);

                        BlobStore::FieldReadInfos infos;
                        BlobStore::FieldReadInfo info(page_internal_id, entry, indices);
                        infos.emplace_back(info);
                        blob_store.read(infos);
                    }
                }
                catch (DB::Exception & e)
                {
                    error_count++;
                    error_msg += e.displayText();
                    error_msg += "\n";
                }
            }
        }

        if (error_count == 0)
        {
            return fmt::format("Checked {} without any error.", page_internal_id);
        }

        error_msg += fmt::format("Check {} meet {} errors!", page_internal_id, error_count);
        return error_msg;
    }

    String checkAllDatasCrc(PageDirectory::MVCCMapType & mvcc_table_directory, BlobStore & blob_store, bool enable_fo_check)
    {
        size_t total_pages = mvcc_table_directory.size();
        size_t cut_index = 0;
        size_t index = 0;
        std::cout << fmt::format("Begin to check all of datas CRC. enable_fo_check={}", (int)enable_fo_check) << std::endl;

        std::list<std::pair<UInt128, PageVersionType>> error_versioned_pages;
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

                        if (enable_fo_check && entry.field_offsets.size() != 0)
                        {
                            DB::PageStorage::FieldIndices indices(entry.field_offsets.size());
                            std::iota(std::begin(indices), std::end(indices), 0);

                            BlobStore::FieldReadInfos infos;
                            BlobStore::FieldReadInfo info(internal_id, entry, indices);
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

        if (error_versioned_pages.size() == 0)
        {
            return "All of data checked. All passed.";
        }

        String error_msg = "Found error in these pages: ";
        for (const auto & [internal_id, versioned] : error_versioned_pages)
        {
            error_msg += fmt::format("id: {}, sequence: {}, epoch: {} \n", internal_id, versioned.sequence, versioned.epoch);
        }
        error_msg += "Please use `--query_table_id` + `--check_page_id` to get the more error info.";

        return error_msg;
    }

private:
    ControlOptions options;
};


} // namespace DB::PS::V3

using namespace DB::PS::V3;
int main(int argc, char ** argv)
{
    const auto & options = ControlOptions::parse(argc, argv);
    PageStorageControl(options).run();
    return 0;
}