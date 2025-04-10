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

#include <Common/Exception.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInputStream_fwd.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/ReadMode.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

class ColumnFile;
using ColumnFilePtr = std::shared_ptr<ColumnFile>;
class ColumnFileReader;
using ColumnFileReaderPtr = std::shared_ptr<ColumnFileReader>;

class ColumnFileInputStream : public SkippableBlockInputStream
{
public: // Implements SkippableBlockInputStream
    bool getSkippedRows(size_t &) override { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

    size_t skipNextBlock() override;

    Block readWithFilter(const IColumn::Filter & filter) override;

public: // Implements IBlockInputStream
    String getName() const override { return "ColumnFile"; }

    Block getHeader() const override;

    // Note: The output block does not contain a start offset.
    Block read() override;

public:
    static ColumnFileInputStreamPtr create(
        const DMContext & context_,
        const ColumnFilePtr & column_file,
        const IColumnFileDataProviderPtr & data_provider_,
        const ColumnDefinesPtr & col_defs_,
        ReadTag read_tag_)
    {
        return std::make_shared<ColumnFileInputStream>(context_, column_file, data_provider_, col_defs_, read_tag_);
    }

    explicit ColumnFileInputStream(
        const DMContext & context_,
        const ColumnFilePtr & column_file,
        const IColumnFileDataProviderPtr & data_provider_,
        const ColumnDefinesPtr & col_defs_,
        ReadTag read_tag_);

private:
    // There could be possibly a lot of ColumnFiles.
    // So we keep this struct as small as possible.

    ColumnDefinesPtr col_defs;
    ColumnFileReaderPtr reader;
};

} // namespace DB::DM
