#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>
#include <Poco/Timer.h>
#include <Storages/Page/PageStorage.h>

void Usage(const char * prog)
{
    fprintf(stderr,
            "Usage: %s <path> <mode>\n"
            "\tmode==1 -> dump all page entries\n"
            "\tmode==2 -> dump valid page entries\n"
            "\tmode==3 -> check all page entries and page data checksum",
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

enum Mode
{
    DUMP_ALL_ENTRIES    = 1,
    DUMP_VALID_ENTRIES  = 2,
    CHECK_DATA_CHECKSUM = 3,
    LIST_ALL_CAPACITY   = 4,
    LIST_ALL_PAGE_FILE  = 5,

    TOTAL_NUM_MODES,
};

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

    Poco::AutoPtr<Poco::ConsoleChannel>   channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel("trace");

    DB::String path     = argv[1];
    DB::String mode_str = argv[2];
    int32_t    mode     = strtol(mode_str.c_str(), nullptr, 10);
    if (mode >= TOTAL_NUM_MODES)
    {
        Usage(argv[0]);
        return 1;
    }

    // Do not remove any files.
    DB::PageStorage::ListPageFilesOption options;
    options.remove_tmp_files  = false;
    options.ignore_legacy     = false;
    options.ignore_checkpoint = false;
    auto page_files           = DB::PageStorage::listAllPageFiles(path, &Poco::Logger::get("root"), options);

    if (mode == LIST_ALL_PAGE_FILE)
    {
        for (auto & page_file : page_files)
        {
            auto [id, level] = page_file.fileIdLevel();
            auto type        = page_file.getType();
            std::cout << "PageFile_" << id << "_" << level //
                      << "\t" << DB::PageFile::typeToString(type) << std::endl;
        }
        return 0;
    }

    //DB::PageEntriesVersionSet versions;
    DB::PageEntriesVersionSetWithDelta versions(DB::MVCC::VersionSetConfig(), &Poco::Logger::get("GetValidPages"));
    for (auto & page_file : page_files)
    {
        DB::PageEntriesEdit        edit;
        DB::PageIdAndEntries       id_and_caches;
        DB::WriteBatch::SequenceID sid = 0;
        const_cast<DB::PageFile &>(page_file).readAndSetPageMetas(edit, sid);

        printf("File: page_%llu_%u with %zu entries:\n", page_file.getFileId(), page_file.getLevel(), edit.size());
        for (const auto & record : edit.getRecords())
        {
            if (mode == DUMP_ALL_ENTRIES)
            {
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
                    printf("DEL\t%lld\t\t@PageFile%llu_%u\n", //
                           record.page_id,
                           page_file.getFileId(),
                           page_file.getLevel());
                    break;
                case DB::WriteBatch::WriteType::REF:
                    printf("REF\t%lld\t%lld\t@PageFile%llu_%u\n", //
                           record.page_id,
                           record.ori_page_id,
                           page_file.getFileId(),
                           page_file.getLevel());
                    break;
                }
            }
        }
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

        versions.apply(edit);
    }

    if (mode == DUMP_VALID_ENTRIES)
    {
        auto snapshot = versions.getSnapshot();
        auto page_ids = snapshot->version()->validPageIds();
        for (auto page_id : page_ids)
        {
            const auto entry = snapshot->version()->find(page_id);
            printPageEntry(page_id, *entry);
        }
    }
    else if (mode == LIST_ALL_CAPACITY)
    {
        constexpr double MB                    = 1.0 * 1024 * 1024;
        auto             snapshot              = versions.getSnapshot();
        auto             valid_normal_page_ids = snapshot->version()->validNormalPageIds();

        std::map<DB::PageFileIdAndLevel, std::pair<size_t, DB::PageIds>> file_valid_pages;
        for (auto page_id : valid_normal_page_ids)
        {
            const auto page_entry = snapshot->version()->findNormalPageEntry(page_id);
            if (unlikely(!page_entry))
            {
                throw DB::Exception("PageStorage GC: Normal Page " + DB::toString(page_id) + " not found.", DB::ErrorCodes::LOGICAL_ERROR);
            }
            auto && [valid_size, valid_page_ids_in_file] = file_valid_pages[page_entry->fileIdLevel()];
            valid_size += page_entry->size;
            valid_page_ids_in_file.emplace_back(page_id);
        }

        size_t global_total_size       = 0;
        size_t global_total_valid_size = 0;

        printf("PageFileId\tPageFileLevel\tPageFileSize\tValidSize\tValidPercent\tNumValidPages\n");
        for (auto & page_file : page_files)
        {
            size_t      total_size   = page_file.getDataFileSize();
            auto        id_and_level = page_file.fileIdLevel();
            size_t      valid_size   = 0;
            DB::PageIds valid_pages;
            if (auto iter = file_valid_pages.find(page_file.fileIdLevel()); iter != file_valid_pages.end())
            {
                valid_size  = iter->second.first;
                valid_pages = iter->second.second;
            }
            global_total_size += total_size;
            global_total_valid_size += valid_size;
            // PageFileId, level, size, valid size, valid percentage
            printf("%9llu\t%9u\t"
                   "%9.2f\t%9.2f\t%9.2f%%\t"
                   "%6zu"
                   "\n",
                   id_and_level.first,
                   id_and_level.second,
                   total_size / MB,
                   valid_size / MB,
                   total_size == 0 ? 0 : (100.0 * valid_size / total_size),
                   valid_pages.size());
        }
        printf("Total size: %.2f MB over %.2f MB\n", global_total_valid_size / MB, global_total_size / MB);
    }
    return 0;
}
catch (const DB::Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    return -1;
}
