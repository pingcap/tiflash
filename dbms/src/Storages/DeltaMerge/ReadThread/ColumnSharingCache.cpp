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
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/ReadThread/ColumnSharingCache.h>

namespace DB::DM
{
DMFileReaderPool & DMFileReaderPool::instance()
{
    static DMFileReaderPool reader_pool;
    return reader_pool;
}

void DMFileReaderPool::add(DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    readers[reader.path()].insert(&reader);
}

void DMFileReaderPool::del(DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(reader.path());
    if (itr == readers.end())
    {
        return;
    }
    itr->second.erase(&reader);
    if (itr->second.empty())
    {
        readers.erase(itr);
    }
}

void DMFileReaderPool::set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(from_reader.path());
    if (itr == readers.end())
    {
        return;
    }
    for (auto * r : itr->second)
    {
        if (&from_reader == r)
        {
            continue;
        }
        r->addCachedPacks(col_id, start, count, col);
    }
}

DMFileReader * DMFileReaderPool::get(const std::string & name)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(name);
    return itr != readers.end() && !itr->second.empty() ? *(itr->second.begin()) : nullptr;
}
} // namespace DB::DM