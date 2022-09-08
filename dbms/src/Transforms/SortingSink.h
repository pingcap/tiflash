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

#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <Transforms/Sink.h>
#include <Transforms/SortBreaker.h>

namespace DB
{
class SortingSink : public Sink
{
public:
    SortingSink(
        const SortDescription & description_,
        size_t limit_,
        const SortBreakerPtr & sort_breaker_)
        : description(description_)
        , limit(limit_)
        , sort_breaker(sort_breaker_)
    {}

    bool write(Block & block, size_t) override;

    void finish() override;

private:
    SortDescription description;
    size_t limit;

    SortBreakerPtr sort_breaker;
    Blocks local_blocks;
};
} // namespace DB
