#include <Encryption/MockKeyManager.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/gc/DataCompactor.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>


/* some exported global vars */
DB::WriteBatch::SequenceID debugging_recover_stop_sequence = 0;
namespace DB
{
#if __APPLE__ && __clang__
__thread bool is_background_thread = false;
#else
thread_local bool is_background_thread = false;
#endif
} // namespace DB
/* some exported global vars */

void Usage(const char * prog)
{
    fprintf(stderr,
            R"HELP(
Usage: %s <path> <mode>
    mode == 1 -> dump all page entries
            2 -> dump valid page entries
              param: %s <path> 2 [max-recover-sequence]
            3 -> check all page entries and page data checksum
            4 -> list capacity of all page files
            5 -> list all page files
            1000 -> gc files
              param: %s <path> 1000 [run-gc-times=1] [min-gc-file-num=10] [min-gc-bytes=134217728] [max-gc-valid-rate=0.35]
)HELP",
            prog,
            prog,
            prog);
}

void printPageEntry(const DB::PageId pid, const DB::PageEntry & entry)
{
    printf("\tpid:%9lld\t\t"
           "%9llu\t%9u\t%9u\t%9llu\t%9llu\t%016llx\n",
           pid, //
           entry.file_id,
           entry.level,
           entry.size,
           entry.offset,
           entry.tag,
           entry.checksum);
}

enum DebugMode
{
    DUMP_ALL_ENTRIES = 1,
    DUMP_VALID_ENTRIES = 2,
    CHECK_DATA_CHECKSUM = 3,
    LIST_ALL_CAPACITY = 4,
    LIST_ALL_PAGE_FILE = 5,

    RUN_GC = 1000,
};

void dump_all_entries(DB::PageFileSet & page_files, int32_t mode = DebugMode::DUMP_ALL_ENTRIES);
void list_all_capacity(const DB::PageFileSet & page_files, DB::PageStorage & storage, const DB::PageStorage::Config & config);

DB::PageStorage::Config parse_storage_config(int argc, char ** argv, Poco::Logger * logger)
{
    DB::PageStorage::Config config;
    if (argc > 4)
    {
        size_t num = strtoull(argv[4], nullptr, 10);
        num = std::max(1UL, num);
        config.gc_min_files = num;
    }
    if (argc > 5)
    {
        size_t num = strtoull(argv[5], nullptr, 10);
        num = std::max(1UL, num);
        config.gc_min_bytes = num;
    }
    if (argc > 6)
    {
        // range from [0.01, 1.0]
        DB::Float64 n = std::stod(argv[6]);
        n = std::min(1.0, std::max(0.01, n));
        config.gc_max_valid_rate = n;
    }

    LOG_INFO(logger,
             "[gc_min_files=" << config.gc_min_files << "] [gc_min_bytes=" << config.gc_min_bytes
                              << "] [gc_max_valid_rate=" << DB::toString(config.gc_max_valid_rate.get(), 3) << "]");

    return config;
}

int main(int argc, char ** argv)
try
{
    (void)argc;
    (void)argv;

    if (argc < 3)
    {
        Usage(argv[0]);
        return 1;
    }

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel("trace");

    DB::String path = argv[1];
    DB::String mode_str = argv[2];
    int32_t mode = strtol(mode_str.c_str(), nullptr, 10);

    Poco::Logger * logger = &Poco::Logger::get("root");

    switch (mode)
    {
    case DUMP_ALL_ENTRIES:
    case DUMP_VALID_ENTRIES:
    case CHECK_DATA_CHECKSUM:
    case LIST_ALL_CAPACITY:
    case LIST_ALL_PAGE_FILE:
    case RUN_GC:
        LOG_INFO(logger, "Running with [mode=" << mode << "]");
        break;
    default:
        Usage(argv[0]);
        return 1;
    }

    if (mode == DUMP_VALID_ENTRIES && argc > 3)
    {
        debugging_recover_stop_sequence = strtoull(argv[3], nullptr, 10);
        LOG_TRACE(logger, "debug early stop sequence set to: " << debugging_recover_stop_sequence);
    }
    DB::KeyManagerPtr key_manager = std::make_shared<DB::MockKeyManager>(false);
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(key_manager, false);
    DB::PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);

    // Do not remove any files.
    DB::PageStorage::ListPageFilesOption options;
    options.remove_tmp_files = false;
    options.ignore_legacy = false;
    options.ignore_checkpoint = false;
    auto page_files = DB::PageStorage::listAllPageFiles(file_provider, delegator, logger, options);
    switch (mode)
    {
    case DUMP_ALL_ENTRIES:
    case CHECK_DATA_CHECKSUM:
        dump_all_entries(page_files, mode);
        return 0;
    case LIST_ALL_PAGE_FILE:
        for (auto & page_file : page_files)
        {
            std::cout << page_file.toString() << std::endl;
        }
        return 0;
    }

    DB::PageStorage::Config config = parse_storage_config(argc, argv, logger);
    DB::PageStorage storage("PageCtl", delegator, config, file_provider);
    storage.restore();
    switch (mode)
    {
    case DUMP_VALID_ENTRIES:
    {
        auto snapshot = storage.getSnapshot();
        auto page_ids = snapshot->version()->validPageIds();
        for (auto page_id : page_ids)
        {
            const auto entry = snapshot->version()->find(page_id);
            printPageEntry(page_id, *entry);
        }
        break;
    }
    case LIST_ALL_CAPACITY:
        list_all_capacity(page_files, storage, config);
        break;
    case RUN_GC:
    {
        Int64 num_gc = 1;
        if (argc > 3)
        {
            num_gc = strtoll(argv[3], nullptr, 10);
            if (num_gc != -1)
                num_gc = std::min(std::max(1, num_gc), 30);
        }
        for (Int64 idx = 0; num_gc == -1 || idx < num_gc; ++idx)
        {
            LOG_INFO(logger, "Running GC, [round=" << (idx + 1) << "] [num_gc=" << num_gc << "]");
            storage.gc(/*not_skip=*/true);
            LOG_INFO(logger, "Run GC done, [round=" << (idx + 1) << "] [num_gc=" << num_gc << "]");
        }
        break;
    }
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl
              << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl
                  << e.getStackTrace().toString() << std::endl;

    return -1;
}

void dump_all_entries(DB::PageFileSet & page_files, int32_t mode)
{
    for (auto & page_file : page_files)
    {
        DB::PageEntriesEdit edit;
        DB::PageIdAndEntries id_and_caches;

        auto reader = DB::PageFile::MetaMergingReader::createFrom(const_cast<DB::PageFile &>(page_file));

        while (reader->hasNext())
        {
            reader->moveNext();
            edit = reader->getEdits();
            auto sequence = reader->writeBatchSequence();
            for (const auto & record : edit.getRecords())
            {
                printf("%s\tseq: %9llu\t", page_file.toString().c_str(), sequence);
                switch (record.type)
                {
                case DB::WriteBatch::WriteType::PUT:
                    printf("PUT");
                    printPageEntry(record.page_id, record.entry);
                    id_and_caches.emplace_back(std::make_pair(record.page_id, record.entry));
                    break;
                case DB::WriteBatch::WriteType::UPSERT:
                    printf("UPSERT");
                    printPageEntry(record.page_id, record.entry);
                    id_and_caches.emplace_back(std::make_pair(record.page_id, record.entry));
                    break;
                case DB::WriteBatch::WriteType::DEL:
                    printf("DEL\t%lld\n", //
                           record.page_id,
                           page_file.getFileId(),
                           page_file.getLevel());
                    break;
                case DB::WriteBatch::WriteType::REF:
                    printf("REF\t%lld\t%lld\t\n", //
                           record.page_id,
                           record.ori_page_id,
                           page_file.getFileId(),
                           page_file.getLevel());
                    break;
                }
            }
        }
        reader->setPageFileOffsets();

        if (mode == CHECK_DATA_CHECKSUM)
        {
            // Read correspond page and check checksum
            auto reader = const_cast<DB::PageFile &>(page_file).createReader();
            try
            {
                fprintf(stderr, "Scanning over data.\n");
                auto page_map = reader->read(id_and_caches);
            }
            catch (DB::Exception & e)
            {
                fprintf(stderr, "%s\n", e.displayText().c_str());
            }
        }
    }
}

void list_all_capacity(const DB::PageFileSet & page_files, DB::PageStorage & storage, const DB::PageStorage::Config & config)
{
    constexpr double MB = 1.0 * 1024 * 1024;

    auto snapshot = storage.getSnapshot();

    DB::DataCompactor<DB::PageStorage::SnapshotPtr>::ValidPages file_valid_pages;
    {
        DB::DataCompactor<DB::PageStorage::SnapshotPtr> compactor(storage, config, nullptr, nullptr);
        file_valid_pages = compactor.collectValidPagesInPageFile(snapshot);
    }

    size_t global_total_size = 0;
    size_t global_total_valid_size = 0;

    printf("PageFileId\tPageFileLevel\tPageFileSize\tValidSize\tValidPercent\tNumValidPages\n");
    for (auto & page_file : page_files)
    {
        if (page_file.getType() != DB::PageFile::Type::Formal)
        {
            printf("%s\n", page_file.toString().c_str());
            continue;
        }

        const size_t total_size = page_file.getDataFileSize();
        size_t valid_size = 0;
        DB::PageIdSet valid_pages;
        if (auto iter = file_valid_pages.find(page_file.fileIdLevel()); iter != file_valid_pages.end())
        {
            valid_size = iter->second.first;
            valid_pages = iter->second.second;
        }
        global_total_size += total_size;
        global_total_valid_size += valid_size;
        // PageFileId, level, size, valid size, valid percentage
        printf("%s\t"
               "%9.2f\t%9.2f\t%9.2f%%\t"
               "%6zu"
               "\n",
               page_file.toString().c_str(),
               total_size / MB,
               valid_size / MB,
               total_size == 0 ? 0 : (100.0 * valid_size / total_size),
               valid_pages.size());
    }
    printf("Total size: %.2f MB over %.2f MB\n", global_total_valid_size / MB, global_total_size / MB);
}
