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
#include <Interpreters/Aggregator.h>

namespace DB
{
class MergingAndConvertingBlockInputStream : public IProfilingBlockInputStream
{
public:
    MergingAndConvertingBlockInputStream(
        const MergingBucketsPtr & merging_buckets_,
        size_t concurrency_index_,
        const String & req_id)
        : merging_buckets(merging_buckets_)
        , concurrency_index(concurrency_index_)
        , log(Logger::get(req_id))
    {}

    String getName() const override { return "MergingAndConverting"; }

    Block getHeader() const override { return merging_buckets->getHeader(); }

protected:
    Block readImpl() override { return merging_buckets->getData(concurrency_index); }

private:
    MergingBucketsPtr merging_buckets;
    size_t concurrency_index;
    const LoggerPtr log;
};
} // namespace DB
