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

#include <DataStreams/SquashingBlockInputStream.h>

namespace DB
{

SquashingBlockInputStream::SquashingBlockInputStream(
    const BlockInputStreamPtr & src,
    size_t min_block_size_rows,
    size_t min_block_size_bytes,
    const String & req_id)
    : log(Logger::get(req_id))
    , transform(min_block_size_rows, min_block_size_bytes, req_id)
{
    children.emplace_back(src);
}


Block SquashingBlockInputStream::read()
{
    if (all_read)
        return {};

    while (true)
    {
        Block block = children[0]->read();
        if (!block)
            all_read = true;

        SquashingTransform::Result result = transform.add(std::move(block));
        if (result.ready)
            return result.block;
    }
}

} // namespace DB
