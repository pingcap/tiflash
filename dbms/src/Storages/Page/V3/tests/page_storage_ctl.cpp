#include <Common/UnifiedLogPatternFormatter.h>
#include <Encryption/MockKeyManager.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V2/gc/DataCompactor.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>

#include <boost/program_options.hpp>

using namespace DB::PS::V3;
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

    static ControlOptions parse(int argc, char ** argv);
};


ControlOptions ControlOptions::parse(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using po::value;

    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "produce help message") //
        ("paths,P", value<std::vector<std::string>>(), "store path(s)") //
        ("display_mode,D", value<int>()->default_value(1), "Display Mode: 1 is summary information,\n 2 is display all of stored page and version chaim(will be very long),\n 3 is display all blobs(in disk) data distribution.") //
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

    if (opt.display_mode < DisplayType::DISPLAY_SUMMARY_INFO || opt.display_mode > DisplayType::CHECK_ALL_DATA_CRC)
    {
        std::cerr << "Invalid display mode: " << opt.display_mode << std::endl;
        std::cerr << desc << std::endl;
        exit(0);
    }

    return opt;
}


void initGlobalLogger()
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new DB::UnifiedLogPatternFormatter);
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel("trace");
}


String getBlobsInfo(BlobStore & blob_store, UInt32 blob_id)
{
    auto statInfo = [](const BlobStore::BlobStats::BlobStatPtr & stat) {
        String stat_str = fmt::format("    stat id: {}\n", stat->id);
        stat_str += fmt::format("      total size: {}\n", stat->sm_total_size);
        stat_str += fmt::format("      valid size: {}\n", stat->sm_valid_size);
        stat_str += fmt::format("      valid rate: {}\n", stat->sm_valid_rate);
        stat_str += fmt::format("      max cap: {}\n", stat->sm_max_caps);

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

String getDirectoryInfo(PageDirectory::MVCCMapType & mvcc_table_directory, UInt64 page_id)
{
    auto pageInfo = [](UInt64 page_id_, const VersionedPageEntriesPtr & versioned_entries) {
        String page_str = fmt::format("    page id {}\n", page_id_);
        page_str += fmt::format("      {}\n", versioned_entries->toDebugString());

        size_t count = 0;
        for (const auto & [version, entry_or_del] : versioned_entries->entries)
        {
            const auto & entry = entry_or_del.entry;
            page_str += fmt::format("        entry {}\n", count++);
            page_str += fmt::format("          sequence: {} \n", version.sequence);
            page_str += fmt::format("          epoch: {} \n", version.epoch);
            page_str += fmt::format("          is del: {} \n", entry_or_del.isDelete());
            page_str += fmt::format("          blob id: {} \n", entry.file_id);
            page_str += fmt::format("          offset: {} \n", entry.offset);
            page_str += fmt::format("          size: {} \n", entry.size);
            page_str += fmt::format("          crc: 0x{:X} \n", entry.checksum);
            page_str += fmt::format("          field offset size: {} \n", entry.field_offsets.size());

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
            if (internal_id.low == page_id)
            {
                directory_str += pageInfo(internal_id.low, versioned_entries);
                return directory_str;
            }
            continue;
        }
        directory_str += pageInfo(internal_id.low, versioned_entries);
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
        stats_str += fmt::format("    stat id: {}\n", stat->id);
        stats_str += fmt::format("      total size: {}\n", stat->sm_total_size);
        stats_str += fmt::format("      valid size: {}\n", stat->sm_valid_size);
        stats_str += fmt::format("      valid rate: {}\n", stat->sm_valid_rate);
        stats_str += fmt::format("      max cap: {}\n", stat->sm_max_caps);
    }


    return dir_summary_info + stats_str;
}

String checkAllDatasCrc(PageDirectory::MVCCMapType & mvcc_table_directory, BlobStore & blob_store)
{
    for (const auto & [internal_id, versioned_entries] : mvcc_table_directory)
    {
        for (const auto & [version, entry_or_del] : versioned_entries->entries)
        {
            if (entry_or_del.isEntry() && versioned_entries->type == EditRecordType::VAR_ENTRY)
            {
                if (internal_id.low == 4927)
                {
                    std::cout << 11 << std::endl;
                }
                try
                {
                    PageIDAndEntryV3 to_read_entry;
                    PageIDAndEntriesV3 to_read;
                    to_read_entry.first = internal_id;
                    to_read_entry.second = entry_or_del.entry;

                    to_read.emplace_back(to_read_entry);
                    blob_store.read(to_read);
                }
                catch (DB::Exception & e)
                {
                    std::cout << e.displayText() << std::endl;
                }
            }
        }
    }
    return "";
}

int main(int argc, char ** argv)
{
    const auto & options = ControlOptions::parse(argc, argv);
    DB::PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(options.paths[0]);

    auto key_manager = std::make_shared<DB::MockKeyManager>(false);
    auto file_provider = std::make_shared<DB::FileProvider>(key_manager, false);

    BlobStore::Config blob_config;
    BlobStore blob_store(file_provider, options.paths[0], blob_config);

    PageDirectoryFactory factory;
    PageDirectoryPtr page_directory = factory.setBlobStore(blob_store)
                                          .create(file_provider, delegator);

    PageDirectory::MVCCMapType & mvcc_table_directory = page_directory->mvcc_table_directory;

    switch (options.display_mode)
    {
    case ControlOptions::DisplayType::DISPLAY_SUMMARY_INFO:
    {
        std::cout << getSummaryInfo(mvcc_table_directory, blob_store) << std::endl;
        break;
    }
    case ControlOptions::DisplayType::DISPLAY_DIRECTORY_INFO:
    {
        std::cout << getDirectoryInfo(mvcc_table_directory, options.query_page_id) << std::endl;
        break;
    }
    case ControlOptions::DisplayType::DISPLAY_BLOBS_INFO:
    {
        std::cout << getBlobsInfo(blob_store, options.query_blob_id) << std::endl;
        break;
    }
    case ControlOptions::DisplayType::CHECK_ALL_DATA_CRC:
    {
        std::cout << checkAllDatasCrc(mvcc_table_directory, blob_store) << std::endl;
        break;
    }
    default:
        std::cout << "Invalid display mode." << std::endl;
        break;
    }

    return 0;
}