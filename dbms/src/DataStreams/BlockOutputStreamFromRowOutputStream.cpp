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

#include <Core/Block.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{
BlockOutputStreamFromRowOutputStream::BlockOutputStreamFromRowOutputStream(
    RowOutputStreamPtr row_output_,
    const Block & header_)
    : row_output(row_output_)
    , header(header_)
{}


void BlockOutputStreamFromRowOutputStream::write(const Block & block)
{
    size_t rows = block.rows();
    size_t columns = block.columns();

    for (size_t i = 0; i < rows; ++i)
    {
        if (!first_row)
            row_output->writeRowBetweenDelimiter();
        first_row = false;

        row_output->writeRowStartDelimiter();

        for (size_t j = 0; j < columns; ++j)
        {
            if (j != 0)
                row_output->writeFieldDelimiter();

            const auto & col = block.getByPosition(j);
            row_output->writeField(*col.column, *col.type, i);
        }

        row_output->writeRowEndDelimiter();
    }
}


void BlockOutputStreamFromRowOutputStream::setRowsBeforeLimit(size_t rows_before_limit)
{
    row_output->setRowsBeforeLimit(rows_before_limit);
}

void BlockOutputStreamFromRowOutputStream::setExtremes(const Block & extremes)
{
    row_output->setExtremes(extremes);
}

void BlockOutputStreamFromRowOutputStream::onProgress(const Progress & progress)
{
    row_output->onProgress(progress);
}

} // namespace DB
