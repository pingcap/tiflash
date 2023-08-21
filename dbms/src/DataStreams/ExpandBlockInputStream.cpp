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

#include <DataStreams/ExpandBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>


namespace DB
{
ExpandBlockInputStream::ExpandBlockInputStream(
    const BlockInputStreamPtr & input,
    const Expand2Ptr & expand2_,
    const Block & header_,
    const String & req_id)
    : expand2(expand2_)
    , header(header_)
    , log(Logger::get(req_id))
{
    children.push_back(input);
    i_th_project = 0;
}

Block ExpandBlockInputStream::getHeader() const
{
    return header;
}

Block ExpandBlockInputStream::readImpl()
{
    if (!block_cache || i_th_project >= expand2->getLevelProjectionNum())
    {
        /// rewind the pos
        i_th_project = 0;
        // cache a new block in ExpandBlockInputStream
        block_cache = children.back()->read();
        if (!block_cache)
            return block_cache;
        // apply before_expand_action when meeting a valid a block.
        expand2->getBeforeExpandActions()->execute(block_cache);
    }
    return expand2->next(block_cache, i_th_project++);
}

} // namespace DB
