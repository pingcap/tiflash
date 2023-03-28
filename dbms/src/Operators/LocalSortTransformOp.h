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

#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>
#include <Operators/Operator.h>

namespace DB
{
enum class LocalSortStatus
{
    PARTIAL,
    MERGE,
};

/// Only do partial and merge sort at the current operator, no sharing of objects with other operators.
class LocalSortTransformOp : public TransformOp
{
public:
    LocalSortTransformOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id_,
        const SortDescription & order_desc_,
        size_t limit_,
        size_t max_block_size_)
        : TransformOp(exec_status_, req_id_)
        , order_desc(order_desc_)
        , limit(limit_)
        , max_block_size(max_block_size_)
    {}

    String getName() const override
    {
        return "LocalSortTransformOp";
    }

    void operatePrefix() override;
    void operateSuffix() override;

protected:
    OperatorStatus transformImpl(Block & block) override;
    OperatorStatus tryOutputImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

private:
    Block getMergeOutput();

private:
    SortDescription order_desc;
    // 0 means no limit.
    size_t limit;
    size_t max_block_size;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// (to avoid excessive virtual function calls and because constants cannot be serialized in Native format for temporary files)
    /// Save original block structure here.
    Block header_without_constants;

    // Used for partial phase.
    // Only a single block is ordered, the global order is not guaranteed.
    Blocks sorted_blocks;
    // Used for merge phase.
    // If there is no output in the merge phase, merge_impl will be nullptr.
    std::unique_ptr<IBlockInputStream> merge_impl;

    LocalSortStatus status{LocalSortStatus::PARTIAL};
};
} // namespace DB
