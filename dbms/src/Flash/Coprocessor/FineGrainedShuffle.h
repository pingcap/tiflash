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

#include <Flash/Coprocessor/DAGContext.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

namespace DB
{
static constexpr std::string_view enableFineGrainedShuffleExtraInfo = "enable fine grained shuffle";

inline bool enableFineGrainedShuffle(uint64_t stream_count)
{
    return stream_count > 0;
}

struct FineGrainedShuffle
{
    explicit FineGrainedShuffle(const tipb::Executor * executor)
        : stream_count(executor ? executor->fine_grained_shuffle_stream_count() : 0)
        , batch_size(executor ? executor->fine_grained_shuffle_batch_size() : 0)
    {}

    bool enable() const
    {
        return enableFineGrainedShuffle(stream_count);
    }

    const UInt64 stream_count;
    const UInt64 batch_size;
};
} // namespace DB
