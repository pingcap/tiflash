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

#include <stdint.h>

#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>


using PageId = uint64_t;
struct PageEntry
{
    // if file_id == 0, means it is invalid
    uint64_t file_id = 0;
    uint32_t level = 0;
    uint32_t size = 0;
    uint64_t offset = 0;
    uint64_t tag = 0;
    uint64_t checksum = 0;
    uint32_t ref = 1; // for ref counting

    inline bool isValid() const { return file_id != 0; }
    inline std::pair<uint64_t, uint32_t> fileIdLevel() const { return std::make_pair(file_id, level); }
};

int main(int argc, char ** argv)
{
    std::string mode;
    size_t num_entries = 9 * 1000 * 1000;
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <map|vec> <num_entries>\n", argv[0]);
        return 1;
    }
    else
    {
        mode = argv[1];
        if (mode != "hash" && mode != "vec" && mode != "tree")
        {
            fprintf(stderr, "Usage: %s <map|tree|vec> <num_entries>\n", argv[0]);
            return 1;
        }
        if (argc >= 3)
        {
            num_entries = strtol(argv[2], nullptr, 10);
        }
    }

    printf("inserting to %s with size: %zu\n", mode.c_str(), num_entries);
    std::unordered_map<PageId, PageEntry> entries_map;
    std::map<PageId, PageEntry> entries_tree_map;
    std::vector<std::pair<PageId, PageEntry>> entries_vec;
    for (size_t i = 0; i < num_entries; ++i)
    {
        if (i % (1000 * 1000) == 0)
            printf("insert %zu done.\n", i);
        if (mode == "hash")
        {
            //  9,000,000 entries 804.6 MB
            // 18,100,100 entries 1.57 GB
            entries_map.insert(std::make_pair(i, PageEntry{.file_id = i}));
        }
        else if (mode == "tree")
        {
            //  9,000,000 entries 837.3 MB
            // 18,000,000 entries 1.64 GB
            entries_tree_map.insert(std::make_pair(i, PageEntry{.file_id = i}));
        }
        else if (mode == "vec")
        {
            //  9,000,000 entries 488.0 MB
            // 18,000,000 entries 968.7 MB
            entries_vec.push_back(std::make_pair(i, PageEntry{.file_id = i}));
        }
    }
    printf("All insert to %s done.\n", mode.c_str());
    std::cin.get();

    return 0;
}
