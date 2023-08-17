// Copyright 2023 PingCAP, Inc.
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

#include <Columns/ColumnConst.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Storages/ColumnDefault.h>


namespace DB
{


/** Adds missing columns to the block with default values.
  * These columns are materialized (not constants).
  */
class AddingDefaultBlockOutputStream : public IBlockOutputStream
{
public:
    AddingDefaultBlockOutputStream(
        const BlockOutputStreamPtr & output_,
        const Block & header_,
        NamesAndTypesList required_columns_,
        const ColumnDefaults & column_defaults_,
        const Context & context_)
        : output(output_)
        , header(header_)
        , required_columns(required_columns_)
        , column_defaults(column_defaults_)
        , context(context_)
    {}

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    BlockOutputStreamPtr output;
    Block header;
    NamesAndTypesList required_columns;
    const ColumnDefaults column_defaults;
    const Context & context;
};


} // namespace DB
