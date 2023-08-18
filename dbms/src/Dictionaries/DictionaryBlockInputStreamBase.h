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
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class DictionaryBlockInputStreamBase : public IProfilingBlockInputStream
{
protected:
    Block block;

    DictionaryBlockInputStreamBase(size_t rows_count, size_t max_block_size);

    virtual Block getBlock(size_t start, size_t length) const = 0;

    Block getHeader() const override;

private:
    const size_t rows_count;
    const size_t max_block_size;
    size_t next_row = 0;

    Block readImpl() override;
};

}
