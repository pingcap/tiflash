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

#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <Operators/Operator.h>

namespace DB
{

class AddExtraTableIDColumnTransformOp : public TransformOp
{
public:
    AddExtraTableIDColumnTransformOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const DM::ColumnDefines & columns_to_read,
        int extra_table_id_index,
        TableID physical_table_id_)
        : TransformOp(exec_context_, req_id)
        , action(columns_to_read, extra_table_id_index)
        , physical_table_id(physical_table_id_)
    {}

    String getName() const override { return "AddExtraTableIDColumnTransformOp"; }

    // `AddExtraTableIDColumnTransformOp` will be appended to `DMSegmentThreadSourceOp` and returned to the computing layer.
    // [`AddExtraTableIDColumnTransformOp` --> `DMSegmentThreadSourceOp`]
    // So override getIOProfileInfo is needed here.
    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

protected:
    OperatorStatus transformImpl(Block & block) override;

    void transformHeaderImpl(Block & header_) override;

private:
    AddExtraTableIDColumnTransformAction action;
    const TableID physical_table_id;
};

} // namespace DB
