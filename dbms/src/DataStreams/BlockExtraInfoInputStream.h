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

/** Adds to one stream additional block information that is specified
  * as the constructor parameter.
  */
class BlockExtraInfoInputStream : public IProfilingBlockInputStream
{
public:
<<<<<<< HEAD:dbms/src/DataStreams/BlockExtraInfoInputStream.h
    BlockExtraInfoInputStream(const BlockInputStreamPtr & input, const BlockExtraInfo & block_extra_info_)
        : block_extra_info(block_extra_info_)
    {
        children.push_back(input);
    }
=======
    MergingAndConvertingBlockInputStream(
        const MergingBucketsPtr & merging_buckets_,
        size_t concurrency_index_,
        const String & req_id)
        : merging_buckets(merging_buckets_)
        , concurrency_index(concurrency_index_)
        , log(Logger::get(req_id))
    {}
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/DataStreams/MergingAndConvertingBlockInputStream.h

    BlockExtraInfo getBlockExtraInfo() const override
    {
        return block_extra_info;
    }

    String getName() const override { return "BlockExtraInfoInput"; }

    Block getHeader() const override { return children.back()->getHeader(); }

protected:
<<<<<<< HEAD:dbms/src/DataStreams/BlockExtraInfoInputStream.h
    Block readImpl() override
    {
        return children.back()->read();
    }
=======
    Block readImpl() override { return merging_buckets->getData(concurrency_index); }
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/DataStreams/MergingAndConvertingBlockInputStream.h

private:
    BlockExtraInfo block_extra_info;
};

}
