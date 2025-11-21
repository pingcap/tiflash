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
    CTESinkOp(PipelineExecutorContext & exec_context_, const String & req_id, std::shared_ptr<CTE> cte_, size_t id_)
        : SinkOp(exec_context_, req_id)
        , cte(cte_)
        , id(id_)
    {}

    String getName() const override { return "CTESinkOp"; }

protected:
    void operateSuffixImpl() override;
    OperatorStatus writeImpl(Block && block) override;

private:
    std::shared_ptr<CTE> cte;
    size_t total_rows = 0;
    size_t id;
};
} // namespace DB
