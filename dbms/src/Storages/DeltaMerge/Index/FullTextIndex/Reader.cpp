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

#include <Common/config.h>

#if ENABLE_CLARA

#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader.h>

namespace DB::DM
{

void FullTextIndexReader::searchNoScore(
    const FullTextIndexPerfPtr & perf,
    rust::Str query,
    const BitmapFilterView & valid_rows,
    rust::Vec<UInt32> & results) const
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);
    SCOPE_EXIT({
        double elapsed = w.elapsedSeconds();
        perf->idx_search_n += 1;
        perf->idx_search_total_ms += elapsed * 1000;
        GET_METRIC(tiflash_fts_index_duration, type_search_noscore).Observe(w.elapsedSeconds());
    });

    // TODO: Utilize match_all=true
    const auto raw_filter = valid_rows.getRawSubFilter(0, valid_rows.size());
    ClaraFTS::BitmapFilter filter;
    filter.match_partial = rust::Slice<const UInt8>(raw_filter.data(), raw_filter.size());
    filter.match_all = false;
    return inner->search_no_score(query, filter, results);
}

void FullTextIndexReader::searchScored(
    const FullTextIndexPerfPtr & perf,
    rust::Str query,
    const BitmapFilterView & valid_rows,
    rust::Vec<ClaraFTS::ScoredResult> & results) const
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);
    SCOPE_EXIT({
        double elapsed = w.elapsedSeconds();
        perf->idx_search_n += 1;
        perf->idx_search_total_ms += elapsed * 1000;
        GET_METRIC(tiflash_fts_index_duration, type_search_scored).Observe(w.elapsedSeconds());
    });

    // TODO: Utilize match_all=true
    const auto raw_filter = valid_rows.getRawSubFilter(0, valid_rows.size());
    ClaraFTS::BitmapFilter filter;
    filter.match_partial = rust::Slice<const UInt8>(raw_filter.data(), raw_filter.size());
    filter.match_all = false;
    return inner->search_scored(query, filter, results);
}
} // namespace DB::DM
#endif
