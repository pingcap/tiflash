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
           "%llu\t%u\t%u\t%9llu\t%llu\t%016llx\n",
           pid, //
           entry.file_id,
           entry.level,
           entry.size,
           entry.offset,
           entry.tag,
           entry.checksum);
}

int main(int argc, char ** argv)
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

    DB::String    path                    = argv[1];
    const int32_t MODE_DUMP_ALL_ENTRIES   = 1;
    const int32_t MODE_DUMP_VALID_ENTRIES = 2;
    DB::String    mode_str                = argv[2];
    int32_t       mode                    = strtol(mode_str.c_str(), nullptr, 10);
    if (mode != MODE_DUMP_ALL_ENTRIES && mode != MODE_DUMP_VALID_ENTRIES)
    {
        Usage(argv[0]);
        return 1;
    }
    auto page_files = DB::PageStorage::listAllPageFiles(path, true, &Poco::Logger::get("root"));

    DB::PageEntryMapVersionSet versions;
    for (auto & page_file : page_files)
    {
        DB::PageEntriesEdit edit;
        DB::PageIdAndEntries id_and_caches;
        const_cast<DB::PageFile &>(page_file).readAndSetPageMetas(edit);

        printf("File: page_%llu_%u with %zu entries:\n", page_file.getFileId(), page_file.getLevel(), edit.size());
        for (const auto & record : edit.getRecords())
        {
            if (mode == MODE_DUMP_ALL_ENTRIES)
            {
                switch (record.type)
                {
                case DB::WriteBatch::WriteType::PUT:
                    printf("PUT");
                    printPageEntry(record.page_id, record.entry);
                    id_and_caches.emplace_back(std::make_pair(record.page_id, record.entry));
                    break;
                case DB::WriteBatch::WriteType::DEL:
                    printf("DEL\t%lld\n", record.page_id);
                    break;
                case DB::WriteBatch::WriteType::REF:
                    printf("REF\t%lld\t%lld\n", record.page_id, record.ori_page_id);
                    break;
                }
            }
        }
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

        versions.apply(edit);
    }

    if (mode == MODE_DUMP_VALID_ENTRIES)
    {
        DB::PageEntryMap * valid_page_entries = versions.currentMap();
        printf("Valid page entries: %zu\n", valid_page_entries->size());
        for (auto iter = valid_page_entries->cbegin(); iter != valid_page_entries->cend(); ++iter)
        {
            const DB::PageId      pid   = iter.pageId();
            const DB::PageEntry & entry = iter.pageEntry();
            printPageEntry(pid, entry);
        }
    }

    return 0;
}
