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

#include <Core/SortDescription.h>
#include <Core/Spiller.h>
#include <DataStreams/IBlockInputStream.h>
#include <Operators/Operator.h>

namespace DB
{
class PartialSortTransformOp : public TransformOp
{
public:
    PartialSortTransformOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id_,
        const SortDescription & order_desc_,
        size_t limit_)
        : TransformOp(exec_context_, req_id_)
        , order_desc(order_desc_)
        , limit(limit_)
    {}

    String getName() const override { return "PartialSortTransformOp"; }

protected:
    ReturnOpStatus transformImpl(Block & block) override;

    void transformHeaderImpl(Block & /*header_*/) override {}

private:
    SortDescription order_desc;
    // 0 means no limit.
    size_t limit;
};
} // namespace DB
