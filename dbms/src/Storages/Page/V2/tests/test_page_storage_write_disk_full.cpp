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

#include <IO/Buffer/ReadBufferFromMemory.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Runnable.h>
#include <Poco/Timer.h>
#include <Storages/Page/V2/PageStorage.h>
#include <common/logger_useful.h>

#include <atomic>
#include <iostream>
#include <memory>
#include <random>

void Usage(const char * prog)
{
    fprintf(stderr, "Usage: %s <limited_path>\n", prog);
}

using PSPtr = std::shared_ptr<DB::PageStorage>;

const DB::PageId MAX_PAGE_ID = 500;

void printPageEntry(const DB::PageId pid, const DB::PageEntry & entry)
{
    printf(
        "\tpid:%9lu\t\t"
        "%9lu\t%u\t%u\t%9lu\t%9lu\t%016lx\n",
        pid, //
        entry.file_id,
        entry.level,
        entry.size,
        entry.offset,
        entry.tag,
        entry.checksum);
}


class PSWriter : public Poco::Runnable
{
private:
    PSPtr ps;
    std::mt19937 gen;

    std::string root;
    // state == 1, writing to fill up disk
    // state == 2, hit disk full
    enum State
    {
        FILLING_UP_DISK = 1,
        HIT_DISK_FULL = 2,
        REWRITE_TO_FILE = 3,
    } state;

public:
    PSWriter(const PSPtr & ps_, const std::string root_)
        : ps(ps_)
        , root(root_)
        , state(FILLING_UP_DISK)
    {}
    void run() override
    {
        while (true)
        {
            if (state == 3)
            {
                break;
            }
            assert(ps != nullptr);
            std::normal_distribution<> d{MAX_PAGE_ID / 2, 150};
            const DB::PageId page_id = static_cast<DB::PageId>(std::round(d(gen))) % MAX_PAGE_ID;

            DB::WriteBatch wb;
            // fill page with random bytes
            const size_t buff_sz = 64 * 1024 * 1024 + random() % 3000;
            char * buff = new char[buff_sz];
            unsigned char buff_ch = pageId % 0xFF; //random() % 0xFF;
            memset(buff, buff_ch, buff_sz);
            wb.putPage(pageId, 0, std::make_shared<DB::ReadBufferFromMemory>(buff, buff_sz), buff_sz);

            LOG_INFO(
                &Poco::Logger::get("root"),
                "writing page" + DB::toString(pageId) + " with size:" + DB::toString(buff_sz)
                    + ", byte: " + DB::toString((DB::UInt32)(buff_ch)));
            try
            {
                ps->write(wb);
                LOG_INFO(
                    &Poco::Logger::get("root"),
                    "writing page" + DB::toString(pageId) + " with size:" + DB::toString(buff_sz) + " done");
                delete[] buff;
            }
            catch (DB::Exception & e)
            {
                delete[] buff;
                if (state == FILLING_UP_DISK)
                {
                    state = HIT_DISK_FULL;

                    // Disk is full, remove one PageFile to free some disk space,
                    // then continue to write one more page
                    const std::string fname = root + "/page_1_0";
                    Poco::File f(fname);
                    f.remove(true);
                }
                else
                {
                    state = REWRITE_TO_FILE;
                }
                LOG_INFO(
                    &Poco::Logger::get("root"),
                    "writing page" + DB::toString(pageId) + " with size:" + DB::toString(buff_sz)
                        + " error: " + e.displayText());
            }
        }
        LOG_INFO(&Poco::Logger::get("root"), "writer exit");
    }
};

int main(int argc, char ** argv)
{
    (void)argc;
    (void)argv;

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel("trace");

    if (argc < 2)
    {
        Usage(argv[0]);
        return 1;
    }
    const DB::String path = argv[1];
    // drop dir if exists
    Poco::File file(path);
    if (file.exists())
        file.remove(true);

    // Create PageStorage
    DB::PageStorageConfig config;
    config.file_roll_size = 96UL * 1024 * 1024;
    PSPtr ps = std::make_shared<DB::PageStorage>(path, config);

    // Write until disk is full
    PSWriter writer(ps, path);
    writer.run();

    // Check if checksum in page is correct.
    auto page_files = DB::PageStorage::listAllPageFiles(path, true, &Poco::Logger::get("root"));
    for (auto & page_file : page_files)
    {
        DB::PS::V2::PageEntries page_entries;
        const_cast<DB::PageFile &>(page_file).readAndSetPageMetas(page_entries, false);
        printf(
            "File: page_%lu_%u with %zu entries:\n",
            page_file.getFileId(),
            page_file.getLevel(),
            page_entries.size());
        DB::PS::V2::PageIdAndEntries id_and_caches;
        for (auto iter = page_entries.cbegin(); iter != page_entries.cend(); ++iter)
        {
            auto pid = iter.pageId();
            auto entry = iter.pageEntry();
            id_and_caches.emplace_back(pid, entry);
            printPageEntry(pid, entry);
        }
        auto reader = const_cast<DB::PageFile &>(page_file).createReader();
        try
        {
            fprintf(stderr, "Scanning over data.\n");
            auto page_map = reader->read(id_and_caches);
        }
        catch (DB::Exception & e)
        {
            fprintf(stderr, "%s\n", e.displayText().c_str());
            return 1; // Error
        }
    }
    return 0;
}
