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
#include <Storages/DeltaMerge/ConcatSkippableBlockInputStream.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/IProvideFullTextIndex.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/InputStream_fwd.h>

namespace DB::DM
{

class BitmapFilter;
using BitmapFilterPtr = std::shared_ptr<BitmapFilter>;

/**
 * @brief Similar to VectorIndexInputStream. See VectorIndexInputStream for details.
 */
class FullTextIndexInputStream : public NopSkippableBlockInputStream
{
public:
    static auto create(
        const FullTextIndexStreamCtxPtr & ctx_,
        const BitmapFilterPtr & bitmap_filter_,
        std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream_)
    {
        return std::make_shared<FullTextIndexInputStream>(ctx_, bitmap_filter_, stream_);
    }

    FullTextIndexInputStream(
        const FullTextIndexStreamCtxPtr & ctx_,
        const BitmapFilterPtr & bitmap_filter_,
        std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream);

public: // Implements IBlockInputStream
    String getName() const override { return "FullTextIndexConcat"; }

    Block getHeader() const override { return stream->getHeader(); }

    Block read() override;

private:
    void onReadFinished();
    bool isReadFinished = false;

private:
    const FullTextIndexStreamCtxPtr ctx;
    const BitmapFilterPtr bitmap_filter;
    const std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream;
    // Assigned in the constructor. Pointers to stream's children, nullptr if the child is not a FullTextIndexBlockInputStream.
    std::vector<IProvideFullTextIndex *> index_streams;

    const LoggerPtr log;

    /// Before returning any actual data, we first perform a vector search over index for substreams.
    void initSearchResults();
    bool searchResultsInited = false;
};

} // namespace DB::DM
#endif
