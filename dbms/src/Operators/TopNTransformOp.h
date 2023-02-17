// Copyright 2023 PingCAP, Ltd.
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
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <Operators/Operator.h>

namespace DB
{
class TopNTransformOp : public TransformOp
{
public:
    TopNTransformOp(
        PipelineExecutorStatus & exec_status_,
        const SortDescription & order_desc_,
        size_t limit_,
        size_t max_block_size_,
        const String & req_id_)
        : TransformOp(exec_status_)
        , log(Logger::get(req_id_))
        , order_desc(order_desc_)
        , limit(limit_)
        , max_block_size(max_block_size_)
        , req_id(req_id_)
    {}

    String getName() const override
    {
        return "TopNTransformOp";
    }

    OperatorStatus transformImpl(Block & block) override;
    OperatorStatus tryOutputImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;


private:
    const LoggerPtr log;
    SortDescription order_desc;
    size_t limit;
    size_t max_block_size;
    String req_id;
    Blocks blocks;
    std::unique_ptr<IBlockInputStream> impl;
};
} // namespace DB
