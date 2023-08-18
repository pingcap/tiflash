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
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>

namespace DB
{
class HashJoinBuildBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "HashJoinBuildBlockInputStream";

public:
    HashJoinBuildBlockInputStream(
        const BlockInputStreamPtr & input,
        JoinPtr join_,
        size_t concurrency_build_index_,
        const String & req_id)
        : concurrency_build_index(concurrency_build_index_)
        , log(Logger::get(NAME, req_id))
    {
        children.push_back(input);
        join = join_;
    }
    String getName() const override { return NAME; }
    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override;

private:
    JoinPtr join;
    size_t concurrency_build_index;
    const LoggerPtr log;
};

} // namespace DB
