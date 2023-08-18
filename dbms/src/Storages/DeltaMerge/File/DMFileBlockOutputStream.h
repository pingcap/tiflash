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

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>

namespace DB
{
namespace DM
{
/// The output stream for writing block to DTFile.
///
/// Note that we will filter block by `RSOperatorPtr` while reading, so the
/// blocks output to DTFile must be bounded by primary key, or we will get
/// wrong results by filtering.
/// You can use `PKSquashingBlockInputStream` to reorganize the boundary of
/// blocks.
class DMFileBlockOutputStream
{
public:
    DMFileBlockOutputStream(const Context & context, const DMFilePtr & dmfile, const ColumnDefines & write_columns);

    DMFilePtr getFile() const { return writer.getFile(); }

    using BlockProperty = DMFileWriter::BlockProperty;
    void write(const Block & block, const BlockProperty & block_property) { writer.write(block, block_property); }

    void writePrefix() {}

    void writeSuffix() { writer.finalize(); }

private:
    DMFileWriter writer;
};

} // namespace DM
} // namespace DB
