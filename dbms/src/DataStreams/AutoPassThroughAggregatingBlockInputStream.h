// Copyright 2024 PingCAP, Inc.
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
#include <Operators/AutoPassThroughHashAggContext.h>

namespace DB
{
static constexpr std::string_view autoPassThroughAggregatingExtraInfo = "auto pass through";

template <bool force_streaming>
class AutoPassThroughAggregatingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "Aggregating";

public:
    AutoPassThroughAggregatingBlockInputStream(
        const BlockInputStreamPtr & input_,
        const Aggregator::Params & params_,
        const String & req_id,
        UInt64 row_limit_unit)
    {
        children.push_back(input_);
        auto_pass_through_context = std::make_unique<AutoPassThroughHashAggContext>(
            children[0]->getHeader(),
            params_,
            [&]() { return this->isCancelled(); },
            req_id,
            row_limit_unit);
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return auto_pass_through_context->getHeader(); }

protected:
    Block readImpl() override;

private:
    AutoPassThroughHashAggContextPtr auto_pass_through_context;
    bool build_done = false;
};

template class AutoPassThroughAggregatingBlockInputStream<true>;
template class AutoPassThroughAggregatingBlockInputStream<false>;
} // namespace DB
