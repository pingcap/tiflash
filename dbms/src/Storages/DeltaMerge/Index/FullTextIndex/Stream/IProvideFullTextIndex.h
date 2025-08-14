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
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader_fwd.h>
#include <common/types.h>

#include <memory>
#include <span>
#include <vector>

namespace DB::DM
{

/**
 * @brief If some inputStream is capable of providing and filtering via a FullTextIndex,
 * then it should inherit and implement this class.
 * inputStream could then only return a subset rows of the matched rows, thank to joining multiple TopK results
 * via FullTextIndexInputStream.
 */
class IProvideFullTextIndex
{
public:
    struct SearchResult
    {
        UInt32 rowid{}; // Always local
        Float32 score{};
    };

    struct SearchResultView
    {
        std::shared_ptr<std::vector<SearchResult>> owner = nullptr;
        std::span<SearchResult> view = {};
    };

public:
    virtual ~IProvideFullTextIndex() = default;

    /// Returns a FullTextIndexReader from the current BlockInputStream.
    virtual FullTextIndexReaderPtr getFullTextIndexReader() = 0;

    /// This inputStream must only return these rows as the final result.
    /// This is always called before the first read().
    /// `return_rows` is ensured to be sorted and does not contain duplicates.
    virtual void setReturnRows(SearchResultView sorted_results) = 0;
};

} // namespace DB::DM
#endif
