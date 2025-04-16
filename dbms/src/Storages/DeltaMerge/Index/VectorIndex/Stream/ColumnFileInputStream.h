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

#include <Storages/DeltaMerge/Index/VectorIndex/Reader_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/ColumnFileInputStream_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/IProvideVectorIndex.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

class ColumnFile;
using ColumnFilePtr = std::shared_ptr<ColumnFile>;
class ColumnFileTiny;
using ColumnFileTinyPtr = std::shared_ptr<ColumnFileTiny>;

class ColumnFileProvideVectorIndexInputStream
    : public IProvideVectorIndex
    , public NopSkippableBlockInputStream
{
public:
    static SkippableBlockInputStreamPtr createOrFallback(
        const VectorIndexStreamCtxPtr & ctx,
        const ColumnFilePtr & column_file);

    ColumnFileProvideVectorIndexInputStream(const VectorIndexStreamCtxPtr & ctx_, const ColumnFileTinyPtr & tiny_file_)
        : ctx(ctx_)
        , tiny_file(tiny_file_)
    {
        RUNTIME_CHECK(tiny_file != nullptr);
    }

public: // Implements IProvideVectorIndex
    VectorIndexReaderPtr getVectorIndexReader() override;

    void setReturnRows(SearchResultView sorted_results_) override { sorted_results = sorted_results_; }

public: // Implements IBlockInputStream
    String getName() const override { return "VectorIndexColumnFile"; }

    Block getHeader() const override;

    // Note: The output block does not contain a start offset.
    Block read() override;

private:
    // Note: Keep this struct small, because it will be constructed for each ColumnFileTiny who has index.
    // If you have common things, put it in ctx. Only put things that are different by each ColumnFileTiny here.

    const VectorIndexStreamCtxPtr ctx;
    const ColumnFileTinyPtr tiny_file;

    VectorIndexReaderPtr vec_index = nullptr;

    // Set by setReturnRows(), clear when a successful read() is done.
    SearchResultView sorted_results; // Used to filter the output and learn what to read from VectorIndex
};

} // namespace DB::DM
