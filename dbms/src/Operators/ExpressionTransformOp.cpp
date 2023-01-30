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

#include <Interpreters/ExpressionActions.h>
#include <Operators/ExpressionTransformOp.h>

namespace DB
{
OperatorStatus ExpressionTransformOp::transformImpl(Block & block)
{
    if (likely(block))
        expression->execute(block);
    return OperatorStatus::HAS_OUTPUT;
}

void ExpressionTransformOp::transformHeaderImpl(Block & header_)
{
    expression->execute(header_);
}
} // namespace DB
