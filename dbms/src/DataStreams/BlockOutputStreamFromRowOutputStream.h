// Copyright 2022 PingCAP, Ltd.
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
#include <DataStreams/IRowOutputStream.h>


namespace DB
{

/** Transforms a stream to write data by rows to a stream to write data by blocks.
  * For example, to write a text dump.
  */
class BlockOutputStreamFromRowOutputStream : public IBlockOutputStream
{
public:
    BlockOutputStreamFromRowOutputStream(RowOutputStreamPtr row_output_, const Block & header_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writePrefix() override { row_output->writePrefix(); }
    void writeSuffix() override { row_output->writeSuffix(); }

    void flush() override { row_output->flush(); }

    void setRowsBeforeLimit(size_t rows_before_limit) override;
    void setExtremes(const Block & extremes) override;
    void onProgress(const Progress & progress) override;

    String getContentType() const override { return row_output->getContentType(); }

private:
    RowOutputStreamPtr row_output;
    Block header;
    bool first_row = true;
};

} // namespace DB
