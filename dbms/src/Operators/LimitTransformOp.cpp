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

#include <DataStreams/LimitTransformAction.h>
#include <Operators/LimitTransformOp.h>

namespace DB
{
template <typename LimitActionPtr>
ReturnOpStatus LimitTransformOp<LimitActionPtr>::transformImpl(Block & block)
{
    if (!action->transform(block))
        block = {};
    return OperatorStatus::HAS_OUTPUT;
}

template <typename LimitActionPtr>
void LimitTransformOp<LimitActionPtr>::transformHeaderImpl(Block & header_)
{
    header_ = action->getHeader();
}

template class LimitTransformOp<GlobalLimitPtr>;
template class LimitTransformOp<LocalLimitPtr>;
} // namespace DB
