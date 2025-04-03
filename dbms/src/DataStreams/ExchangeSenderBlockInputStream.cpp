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

#include <DataStreams/ExchangeSenderBlockInputStream.h>

namespace DB
{
Block ExchangeSenderBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    if (block)
    {
        total_rows += block.rows();
        writer->write(block);
        assert(!writer->hasPendingFlush());
    }
    else
    {
        writer->flush();
        assert(!writer->hasPendingFlush());
    }
    return block;
}
} // namespace DB
