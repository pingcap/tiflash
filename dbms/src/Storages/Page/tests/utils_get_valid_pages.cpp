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
            "\tmode==2 -> dump valid page entries\n",
            prog);
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
        printf("File: page_%llu_%llu with %llu entries:\n", page_file.getFileId(), page_file.getLevel(), page_entries.size());
        for (auto & [pid, entry] : page_entries)
        {
            if (mode == MODE_DUMP_ALL_ENTRIES)
            {
                printf("\tpid:%9lld\t\t"
                       "%llu\t%llu\t%llu\t%9llu\t%llu\t%016llx\n",
                       pid, //
                       entry.file_id,
                       entry.level,
                       entry.size,
                       entry.offset,
                       entry.tag,
                       entry.checksum);
            }
            valid_page_entries[pid] = entry;
        }
    }

    if (mode == MODE_DUMP_VALID_ENTRIES)
    {
        printf("Valid page entries: %lld\n", valid_page_entries.size());
        for (auto & [pid, entry] : valid_page_entries)
        {
            printf("\tpid:%9lld\t\t"
                   "%llu\t%llu\t%llu\t%9llu\t%llu\t%016llx\n",
                   pid, //
                   entry.file_id,
                   entry.level,
                   entry.size,
                   entry.offset,
                   entry.tag,
                   entry.checksum);
        }
    }

    return 0;
}
