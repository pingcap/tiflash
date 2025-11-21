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
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/DMFileInputStream_fwd.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/IProvideFullTextIndex.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>


namespace DB::DM
{

/**
 * @brief DMFileInputStreamProvideFullTextIndex is similar to
 * DMFileInputStreamProvideVectorTextIndex.
 *
 * Before constructing this class, the caller must ensure that full text index
 * exists on the corresponding column. If the index does not exist, the caller
 * should use the standard DMFileBlockInputStream.
 */
class DMFileInputStreamProvideFullTextIndex
    : public IProvideFullTextIndex
    , public NopSkippableBlockInputStream
{
public:
    static auto create(const FullTextIndexStreamCtxPtr & ctx, const DMFilePtr & dmfile, DMFileReader && rest_col_reader)
    {
        return std::make_shared<DMFileInputStreamProvideFullTextIndex>(ctx, dmfile, std::move(rest_col_reader));
    }

    explicit DMFileInputStreamProvideFullTextIndex(
        const FullTextIndexStreamCtxPtr & ctx_,
        const DMFilePtr & dmfile_,
        DMFileReader && rest_col_reader_);

public: // Implements IProvideFullTextIndex
    FullTextIndexReaderPtr getFullTextIndexReader() override;

    void setReturnRows(IProvideFullTextIndex::SearchResultView sorted_results) override;

public: // Implements IBlockInputStream
    Block read() override;

    String getName() const override { return "FullTextIndexDMFile"; }

    Block getHeader() const override;

private:
    // Update the read_block_infos according to the sorted_results.
    void updateReadBlockInfos();

private:
    const FullTextIndexStreamCtxPtr ctx;
    const DMFilePtr dmfile;
    FullTextIndexReaderPtr fts_index = nullptr;
    // Vector column should be excluded in the reader
    DMFileReader rest_col_reader;

    /// Set after calling setReturnRows
    IProvideFullTextIndex::SearchResultView sorted_results;
};

} // namespace DB::DM
#endif
