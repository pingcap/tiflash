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

#include <Common/Logger.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Transforms/FinalAggregateReader.h>
#include <Interpreters/AggregateStore.h>

namespace DB
{
class FinalAggregatingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "FinalAggregating";

public:
    FinalAggregatingBlockInputStream(
        const FinalAggregateReaderPtr & final_agg_reader_,
        const String & req_id)
        : final_agg_reader(final_agg_reader_)
        , log(Logger::get(NAME, req_id))
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

    FinalAggregateReaderPtr final_agg_reader;

    const LoggerPtr log;    
};
} // namespace DB
