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
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V2/gc/DataCompactor.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>

using namespace DB::PS::V2;
DB::WriteBatch::SequenceID debugging_recover_stop_sequence = 0;

void Usage()
{
    fprintf(stderr,
            R"HELP(
Usage: <path> <mode>
    mode == 1 -> dump all page entries
            2 -> dump valid page entries
              param: <path> 2 [max-recover-sequence]
            3 -> check all page entries and page data checksum
            4 -> list capacity of all page files
            5 -> list all page files
            1000 -> gc files
              param: <path> 1000 [run-gc-times=1] [min-gc-file-num=10] [min-gc-bytes=134217728] [max-gc-valid-rate=0.35]
            )HELP");
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

void dump_all_entries(PageFileSet & page_files, int32_t mode = DebugMode::DUMP_ALL_ENTRIES);
void list_all_capacity(const PageFileSet & page_files, PageStorage & storage, const DB::PageStorageConfig & config);

DB::PageStorageConfig parse_storage_config(int argc, char ** argv, Poco::Logger * logger)
{
    DB::PageStorageConfig config;
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

    LOG_INFO(
        logger,
        "[gc_min_files={}] [gc_min_bytes={}] [gc_max_valid_rate={:.3f}]",
        config.gc_min_files,
        config.gc_min_bytes,
        config.gc_max_valid_rate.get());
    return config;
}

int pageStorageV2CtlEntry(int argc, char ** argv)
try
{
    (void)argc;
    (void)argv;

    if (argc < 3)
    {
        Usage();
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
        LOG_INFO(logger, "Running with [mode={}]", mode);
        break;
    default:
        Usage();
        return 1;
    }

    if (mode == DUMP_VALID_ENTRIES && argc > 3)
    {
        debugging_recover_stop_sequence = strtoull(argv[3], nullptr, 10);
        LOG_TRACE(logger, "debug early stop sequence set to: {}", debugging_recover_stop_sequence);
    }
    DB::KeyManagerPtr key_manager = std::make_shared<DB::MockKeyManager>(false);
    DB::FileProviderPtr file_provider = std::make_shared<DB::FileProvider>(key_manager, false);
    DB::PSDiskDelegatorPtr delegator = std::make_shared<DB::tests::MockDiskDelegatorSingle>(path);

    // Do not remove any files.
    PageStorage::ListPageFilesOption options;
    options.remove_tmp_files = false;
    options.ignore_legacy = false;
    options.ignore_checkpoint = false;
    auto page_files = PageStorage::listAllPageFiles(file_provider, delegator, logger, options);
    switch (mode)
    {
    case DUMP_ALL_ENTRIES:
    case CHECK_DATA_CHECKSUM:
        dump_all_entries(page_files, mode);
        return 0;
    case LIST_ALL_PAGE_FILE:
        for (const auto & page_file : page_files)
        {
            std::cout << page_file.toString() << std::endl;
        }
        return 0;
    }

    auto bkg_pool = std::make_shared<DB::BackgroundProcessingPool>(4, "bg-page-");
    DB::PageStorageConfig config = parse_storage_config(argc, argv, logger);
    PageStorage storage("PageCtl", delegator, config, file_provider, *bkg_pool);
    storage.restore();
    switch (mode)
    {
    case DUMP_VALID_ENTRIES:
    {
        auto snapshot = storage.getConcreteSnapshot();
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
            LOG_INFO(logger, "Running GC, [round={}] [num_gc={}]", (idx + 1), num_gc);
            storage.gcImpl(/*not_skip=*/true, nullptr, nullptr);
            LOG_INFO(logger, "Run GC done, [round={}] [num_gc={}]", (idx + 1), num_gc);
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

void dump_all_entries(PageFileSet & page_files, int32_t mode)
{
    for (const auto & page_file : page_files)
    {
        PageEntriesEdit edit;
        DB::PageIdAndEntries id_and_caches;

        auto reader = PageFile::MetaMergingReader::createFrom(const_cast<PageFile &>(page_file));

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
                case DB::WriteBatchWriteType::PUT_EXTERNAL:
                case DB::WriteBatchWriteType::PUT:
                    printf("PUT");
                    printPageEntry(record.page_id, record.entry);
                    id_and_caches.emplace_back(std::make_pair(record.page_id, record.entry));
                    break;
                case DB::WriteBatchWriteType::UPSERT:
                    printf("UPSERT");
                    printPageEntry(record.page_id, record.entry);
                    id_and_caches.emplace_back(std::make_pair(record.page_id, record.entry));
                    break;
                case DB::WriteBatchWriteType::DEL:
                    printf("DEL\t%lld\t%llu\t%u\n", //
                           record.page_id,
                           page_file.getFileId(),
                           page_file.getLevel());
                    break;
                case DB::WriteBatchWriteType::REF:
                    printf("REF\t%lld\t%lld\t\t%llu\t%u\n", //
                           record.page_id,
                           record.ori_page_id,
                           page_file.getFileId(),
                           page_file.getLevel());
                    break;
                case DB::WriteBatchWriteType::PUT_REMOTE:
                    break;
                }
            }
        }
        reader->setPageFileOffsets();

        if (mode == CHECK_DATA_CHECKSUM)
        {
            // Read correspond page and check checksum
            auto reader = const_cast<PageFile &>(page_file).createReader();
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

void list_all_capacity(const PageFileSet & page_files, PageStorage & storage, const DB::PageStorageConfig & config)
{
    static constexpr double MB = 1.0 * 1024 * 1024;

    auto snapshot = storage.getConcreteSnapshot();

    DataCompactor<PageStorage::ConcreteSnapshotPtr>::ValidPages file_valid_pages;
    {
        DataCompactor<PageStorage::ConcreteSnapshotPtr> compactor(storage, config, nullptr, nullptr);
        file_valid_pages = compactor.collectValidPagesInPageFile(snapshot);
    }

    size_t global_total_size = 0;
    size_t global_total_valid_size = 0;

    printf("PageFileId\tPageFileLevel\tPageFileSize\tValidSize\tValidPercent\tNumValidPages\n");
    for (const auto & page_file : page_files)
    {
        if (page_file.getType() != PageFile::Type::Formal)
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
