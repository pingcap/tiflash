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
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/BruteScoreInputStream_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

/// Accepts data containing Full Text column from the underlying stream and calculate a score on-demand.
/// It always outputs a schema that matches TiDB's demand.
/// The underlying stream must output a schema like `ctx.noindex_read_schema`.
///
/// Note: This stream will only produce MVCC filtered rows, and the output block does not have a startOffset.
class FullTextBruteScoreInputStream : public NopSkippableBlockInputStream
{
public:
    static FullTextBruteScoreInputStreamPtr create(
        const FullTextIndexStreamCtxPtr & ctx_,
        SkippableBlockInputStreamPtr stream_)
    {
        return std::make_shared<FullTextBruteScoreInputStream>(ctx_, stream_);
    }

public:
    FullTextBruteScoreInputStream(const FullTextIndexStreamCtxPtr & ctx_, SkippableBlockInputStreamPtr stream_)
        : ctx(ctx_)
    {
        RUNTIME_CHECK(stream_ != nullptr);
        children.emplace_back(stream_);
    }

    Block getHeader() const override;

    String getName() const override { return "BruteScore"; }

    Block read() override;

private:
    friend class FullTextIndexInputStream;

    // Called by FullTextIndexInputStream only. It knows the BitmapFilter.
    void setBitmapFilter(BitmapFilterView bitmap_filter_) { bitmap_filter = bitmap_filter_; }

private:
    const FullTextIndexStreamCtxPtr ctx;

    std::optional<BitmapFilterView> bitmap_filter;
};

} // namespace DB::DM
#endif
