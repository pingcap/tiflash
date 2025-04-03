// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Mpp/CTEManager.h>
#include <Operators/CTE.h>
#include <Operators/Operator.h>

namespace DB
{
class CTESinkOp : public SinkOp
{
public:
    CTESinkOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const String & query_id_and_cte_id_,
        CTEManager * cte_manager_)
        : SinkOp(exec_context_, req_id)
        , query_id_and_cte_id(query_id_and_cte_id_)
        , cte_manager(cte_manager_)
        , cte(cte_manager_->getCTE(query_id_and_cte_id_))
    {}

    String getName() const override { return "CTESinkOp"; }
    bool canHandleSelectiveBlock() const override { return true; }

protected:
    void operateSuffixImpl() override;
    OperatorStatus writeImpl(Block && block) override;

private:
    String query_id_and_cte_id;
    CTEManager * cte_manager;
    std::shared_ptr<CTE> cte;
    size_t total_rows = 0;
    bool input_done = false;
};
} // namespace DB
