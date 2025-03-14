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

#include <Storages/DeltaMerge/ConcatSkippableBlockInputStream.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/IProvideVectorIndex.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/InputStream_fwd.h>

namespace DB::DM
{

class BitmapFilter;
using BitmapFilterPtr = std::shared_ptr<BitmapFilter>;

/**
 * @brief Unifies multiple sub-streams that supports reading the vector index.
 * It first performs a vector search over index for all substreams.
 * Then, a final TopK result is formed.
 * After that, it reads the actual data with only selected TopK rows from these substreams.
 * This class ensures the output result always respect the BitmapFilter.
 */
class VectorIndexInputStream : public NopSkippableBlockInputStream
{
public:
    static auto create(
        const VectorIndexStreamCtxPtr & ctx_,
        const BitmapFilterPtr & bitmap_filter_,
        std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream_)
    {
        return std::make_shared<VectorIndexInputStream>(ctx_, bitmap_filter_, stream_);
    }

    VectorIndexInputStream(
        const VectorIndexStreamCtxPtr & ctx_,
        const BitmapFilterPtr & bitmap_filter_,
        std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream);

    ~VectorIndexInputStream() override;

public: // Implements IBlockInputStream
    String getName() const override { return "VectorIndexConcat"; }

    Block getHeader() const override { return stream->getHeader(); }

    Block read() override;

private:
    void onReadFinished();
    bool isReadFinished = false;

private:
    const VectorIndexStreamCtxPtr ctx;
    const BitmapFilterPtr bitmap_filter;
    const std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream;
    // Assigned in the constructor. Pointers to stream's children, nullptr if the child is not a VectorIndexBlockInputStream.
    std::vector<IProvideVectorIndex *> index_streams;

    const LoggerPtr log;

    /// Before returning any actual data, we first perform a vector search over index for substreams.
    void initSearchResults();
    bool searchResultsInited = false;
};

} // namespace DB::DM
