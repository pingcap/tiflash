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

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/SquashingTransform.h>


namespace DB
{

/** Merging consecutive blocks of stream to specified minimum size.
  */
class SquashingBlockOutputStream : public IBlockOutputStream
{
public:
    SquashingBlockOutputStream(BlockOutputStreamPtr & dst, size_t min_block_size_rows, size_t min_block_size_bytes);

    Block getHeader() const override { return output->getHeader(); }
    void write(const Block & block) override;

    void flush() override;
    void writePrefix() override;
    void writeSuffix() override;

    /// Don't write blocks less than specified size even when flush method was called by user.
    void disableFlush() { disable_flush = true; }

private:
    BlockOutputStreamPtr output;

    SquashingTransform transform;
    bool all_written = false;

    void finalize();

    bool disable_flush = false;
};

} // namespace DB
