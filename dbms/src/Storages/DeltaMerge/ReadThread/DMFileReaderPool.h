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

#pragma once

#include <Storages/DeltaMerge/File/ColumnCache.h>


namespace DB::DM
{
using PackId = size_t;

class DMFileReader;

class DMFileReaderPoolSharding
{
public:
    void add(const String & path, DMFileReader & reader);
    void del(const String & path, DMFileReader & reader);
    void set(
        const String & path,
        DMFileReader & from_reader,
        int64_t col_id,
        size_t start,
        size_t count,
        ColumnPtr & col);
    bool hasConcurrentReader(const String & path);
    // `get` is just for test.
    DMFileReader * get(const std::string & path);

private:
    std::mutex mtx;
    std::unordered_map<std::string, std::unordered_set<DMFileReader *>> readers;
};

// DMFileReaderPool holds all the DMFileReader objects, so we can easily find DMFileReader objects with the same DMFile ID.
// When a DMFileReader object successfully reads a column's packs, it will try to put these packs into other DMFileReader objects' cache.
class DMFileReaderPool
{
public:
    static DMFileReaderPool & instance();
    ~DMFileReaderPool() = default;
    DISALLOW_COPY_AND_MOVE(DMFileReaderPool);

    void add(DMFileReader & reader);
    void del(DMFileReader & reader);
    void set(DMFileReader & from_reader, int64_t col_id, size_t start, size_t count, ColumnPtr & col);
    bool hasConcurrentReader(DMFileReader & from_reader);
    // `get` is just for test.
    DMFileReader * get(const std::string & path);

private:
    DMFileReaderPool() = default;
    DMFileReaderPoolSharding & getSharding(const String & path);

private:
    std::array<DMFileReaderPoolSharding, 16> shardings;
};

} // namespace DB::DM
