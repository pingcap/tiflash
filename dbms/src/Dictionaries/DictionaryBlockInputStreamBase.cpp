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

#include <Dictionaries/DictionaryBlockInputStreamBase.h>

namespace DB
{

DictionaryBlockInputStreamBase::DictionaryBlockInputStreamBase(size_t rows_count, size_t max_block_size)
    : rows_count(rows_count), max_block_size(max_block_size), next_row(0)
{
}

Block DictionaryBlockInputStreamBase::readImpl()
{
    if (next_row == rows_count)
        return Block();

    size_t block_size = std::min<size_t>(max_block_size, rows_count - next_row);
    Block block = getBlock(next_row, block_size);
    next_row += block_size;
    return block;
}

Block DictionaryBlockInputStreamBase::getHeader() const
{
    return getBlock(0, 0);
}

}
