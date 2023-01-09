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
#include <Operators/Operator.h>

namespace DB
{
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionTransform : public TransformOp
{
public:
    explicit ExpressionTransform(
        const ExpressionActionsPtr & expression_,
        const String & req_id)
        : expression(expression_)
        , log(Logger::get(req_id))
    {}

    OperatorStatus transform(Block & block) override;

private:
    ExpressionActionsPtr expression;
    const LoggerPtr log;
};
} // namespace DB
