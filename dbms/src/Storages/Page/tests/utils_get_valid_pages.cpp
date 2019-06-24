#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
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

void printPageEntry(const DB::PageId pid, const DB::PageCache & entry)
{
    printf("\tpid:%9lu\t\t"
           "%9lu\t%u\t%u\t%9lu\t%9lu\t%016lx\n",
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
    Logger::root().setChannel(formatting_channel);
    Logger::root().setLevel("trace");

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
    auto page_files = DB::PageStorage::listAllPageFiles(path, true, &Logger::get("root"));

    DB::PageCacheMap valid_page_entries;
    for (auto & page_file : page_files)
    {
        DB::PageCacheMap page_entries;
        const_cast<DB::PageFile &>(page_file).readAndSetPageMetas(page_entries);
        printf("File: page_%lu_%u with %zu entries:\n", page_file.getFileId(), page_file.getLevel(), page_entries.size());
        DB::PageIdAndCaches id_and_caches;
        for (auto & [pid, entry] : page_entries)
        {
            id_and_caches.emplace_back(pid, entry);
            if (mode == MODE_DUMP_ALL_ENTRIES)
            {
                printPageEntry(pid, entry);
            }
            valid_page_entries[pid] = entry;
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
    }

    if (mode == MODE_DUMP_VALID_ENTRIES)
    {
        printf("Valid page entries: %zu\n", valid_page_entries.size());
        for (auto & [pid, entry] : valid_page_entries)
        {
            printPageEntry(pid, entry);
        }
    }

    return 0;
}
