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

#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/DMFileInputStream_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/IProvideVectorIndex.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>


namespace DB::DM
{

/**
 * @brief DMFileInputStreamProvideVectorIndex is similar to DMFileBlockInputStream.
 * However it can read data efficiently with the help of vector index.
 *
 * General steps:
 * 1. Read all PK, Version and Del Marks (respecting Pack filters).
 * 2. Construct a bitmap of valid rows (in memory). This bitmap guides the reading of vector index to determine whether a row is valid or not.
 * 3. Perform a vector search for Top K vector rows. We now have K row_ids whose vector distance is close.
 * 4. Map these row_ids to packids as the new pack filter.
 * 5. Read from other columns with the new pack filter.
 *     For each read, join other columns and vector column together.
 *
 *  Step 3~4 is performed lazily at first read.
 *
 * Before constructing this class, the caller must ensure that vector index
 * exists on the corresponding column. If the index does not exist, the caller
 * should use the standard DMFileBlockInputStream.
 */
class DMFileInputStreamProvideVectorIndex
    : public IProvideVectorIndex
    , public NopSkippableBlockInputStream
{
public:
    static auto create(const VectorIndexStreamCtxPtr & ctx, const DMFilePtr & dmfile, DMFileReader && rest_col_reader)
    {
        return std::make_shared<DMFileInputStreamProvideVectorIndex>(ctx, dmfile, std::move(rest_col_reader));
    }

    explicit DMFileInputStreamProvideVectorIndex(
        const VectorIndexStreamCtxPtr & ctx_,
        const DMFilePtr & dmfile_,
        DMFileReader && rest_col_reader_);

public: // Implements IProvideVectorIndex
    VectorIndexReaderPtr getVectorIndexReader() override;

    void setReturnRows(IProvideVectorIndex::SearchResultView sorted_results) override;

public: // Implements IBlockInputStream
    Block read() override;

    String getName() const override { return "VectorIndexDMFile"; }

    Block getHeader() const override;

private:
    // Update the read_block_infos according to the sorted_results.
    void updateReadBlockInfos();

private:
    const VectorIndexStreamCtxPtr ctx;
    const DMFilePtr dmfile;
    VectorIndexReaderPtr vec_index = nullptr;
    // Vector column should be excluded in the reader
    DMFileReader rest_col_reader;

    /// Set after calling setReturnRows
    IProvideVectorIndex::SearchResultView sorted_results;
};

} // namespace DB::DM
