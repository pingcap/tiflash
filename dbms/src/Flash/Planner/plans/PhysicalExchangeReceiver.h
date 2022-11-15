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

#include <Flash/Mpp/ExchangeReceiver.h>
#include <Flash/Planner/plans/PhysicalLeaf.h>

namespace DB
{
class ExchangeReceiver;

class PhysicalExchangeReceiver : public PhysicalLeaf
{
public:
    static PhysicalPlanNodePtr build(
        const Context & context,
        const String & executor_id,
        const LoggerPtr & log);

    PhysicalExchangeReceiver(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & req_id,
        const Block & sample_block_,
        const std::shared_ptr<ExchangeReceiver> & mpp_exchange_receiver_);

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

    size_t getSourceNum() const
    {
        return mpp_exchange_receiver->getSourceNum();
    }

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    Block sample_block;

    std::shared_ptr<ExchangeReceiver> mpp_exchange_receiver;
};
} // namespace DB
