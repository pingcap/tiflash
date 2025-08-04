// Copyright 2025 PingCAP, Inc.
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

#include <Common/config.h>

#if ENABLE_CLARA
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader_fwd.h>
#include <Storages/DeltaMerge/Index/ICacheableLocalIndexReader.h>
#include <clara_fts/src/index_reader.rs.h>
#include <common/types.h>

namespace DB::DM
{

class FullTextIndexReader : public ICacheableLocalIndexReader
{
public:
    static FullTextIndexReaderPtr createFromMmap(rust::Str index_path)
    {
        return std::make_shared<FullTextIndexReader>(ClaraFTS::new_mmap_index_reader(index_path));
    }

    static FullTextIndexReaderPtr createFromMemory(rust::Slice<const UInt8> data)
    {
        return std::make_shared<FullTextIndexReader>(ClaraFTS::new_memory_index_reader_2(data));
    }

    explicit FullTextIndexReader(rust::Box<ClaraFTS::IndexReader> && inner_)
        : inner(std::move(inner_))
    {}

    void searchNoScore(
        const FullTextIndexPerfPtr & perf,
        rust::Str query,
        const BitmapFilterView & valid_rows,
        rust::Vec<UInt32> & results) const;

    void searchScored(
        const FullTextIndexPerfPtr & perf,
        rust::Str query,
        const BitmapFilterView & valid_rows,
        rust::Vec<ClaraFTS::ScoredResult> & results) const;

    auto get(UInt32 doc_id, rust::String & result) const { return inner->get(doc_id, result); }

private:
    rust::Box<ClaraFTS::IndexReader> inner;
};

} // namespace DB::DM
#endif
