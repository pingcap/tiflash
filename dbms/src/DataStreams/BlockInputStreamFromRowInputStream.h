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

#include <Core/Defines.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IRowInputStream.h>


namespace DB
{

/** Makes block-oriented stream on top of row-oriented stream.
  * It is used to read data from text formats.
  *
  * Also controls over parsing errors and prints diagnostic information about them.
  */
class BlockInputStreamFromRowInputStream : public IProfilingBlockInputStream
{
public:
    /** sample_ - block with zero rows, that structure describes how to interpret values */
    BlockInputStreamFromRowInputStream(
        const RowInputStreamPtr & row_input_,
        const Block & sample_,
        size_t max_block_size_,
        UInt64 allow_errors_num_,
        Float64 allow_errors_ratio_);

    void readPrefix() override { row_input->readPrefix(); }
    void readSuffix() override { row_input->readSuffix(); }

    String getName() const override { return "BlockInputStreamFromRowInputStream"; }

    RowInputStreamPtr & getRowInput() { return row_input; }

    Block getHeader() const override { return sample; }

protected:
    Block readImpl() override;

private:
    RowInputStreamPtr row_input;
    Block sample;
    size_t max_block_size;

    UInt64 allow_errors_num;
    Float64 allow_errors_ratio;

    size_t total_rows = 0;
    size_t num_errors = 0;
};

} // namespace DB
