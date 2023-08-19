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

#pragma once

#include <Storages/Page/Page.h>
namespace DB::PS::V1
{
struct Page
{
    PageId page_id;
    ByteBuffer data;

    MemHolder mem_holder;
};
using Pages = std::vector<Page>;
using PageMap = std::map<PageId, Page>;
using PageHandler = std::function<void(PageId page_id, const Page &)>;

// Indicate the page size && offset in PageFile.
struct PageEntry
{
    // if file_id == 0, means it is invalid
    PageFileId file_id = 0;
    PageSize size = 0;
    UInt64 offset = 0;
    UInt64 tag = 0;
    UInt64 checksum = 0;
    UInt32 level = 0;
    UInt32 ref = 1; // for ref counting

    inline bool isValid() const { return file_id != 0; }
    inline bool isTombstone() const { return ref == 0; }
    inline PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
};
static_assert(std::is_trivially_copyable_v<PageEntry>);

using PageIdAndEntry = std::pair<PageId, PageEntry>;
using PageIdAndEntries = std::vector<PageIdAndEntry>;

} // namespace DB::PS::V1
