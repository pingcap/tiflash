// Copyright 2024 PingCAP, Inc.
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
#include <Storages/DeltaMerge/ReadThread/DMFileReaderPool.h>

namespace DB::DM
{

void DMFileReaderPoolSharding::add(const String & path, DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    readers[path].insert(&reader);
}

void DMFileReaderPoolSharding::del(const String & path, DMFileReader & reader)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(path);
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

void DMFileReaderPoolSharding::set(
    const String & path,
    DMFileReader & from_reader,
    int64_t col_id,
    size_t start,
    size_t count,
    ColumnPtr & col)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(path);
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
        r->addColumnToCache(r->data_sharing_col_data_cache, col_id, start, count, col);
    }
}

// Check is there any concurrent DMFileReader with `from_reader`.
bool DMFileReaderPoolSharding::hasConcurrentReader(const String & path)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(path);
    return itr != readers.end() && itr->second.size() >= 2;
}

DMFileReader * DMFileReaderPoolSharding::get(const std::string & path)
{
    std::lock_guard lock(mtx);
    auto itr = readers.find(path);
    return itr != readers.end() && !itr->second.empty() ? *(itr->second.begin()) : nullptr;
}

DMFileReaderPool & DMFileReaderPool::instance()
{
    static DMFileReaderPool reader_pool;
    return reader_pool;
}

DMFileReaderPoolSharding & DMFileReaderPool::getSharding(const String & path)
{
    return shardings[std::hash<String>{}(path) % shardings.size()];
}

void DMFileReaderPool::add(DMFileReader & reader)
{
    auto path = reader.path();
    getSharding(path).add(path, reader);
}

void DMFileReaderPool::del(DMFileReader & reader)
{
    auto path = reader.path();
    getSharding(path).del(path, reader);
}

void DMFileReaderPool::set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col)
{
    auto path = from_reader.path();
    getSharding(path).set(path, from_reader, col_id, start, count, col);
}

// Check is there any concurrent DMFileReader with `from_reader`.
bool DMFileReaderPool::hasConcurrentReader(DMFileReader & from_reader)
{
    auto path = from_reader.path();
    return getSharding(path).hasConcurrentReader(path);
}

DMFileReader * DMFileReaderPool::get(const std::string & path)
{
    return getSharding(path).get(path);
}
} // namespace DB::DM
