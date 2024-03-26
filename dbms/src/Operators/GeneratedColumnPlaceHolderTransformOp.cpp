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
#include <Operators/GeneratedColumnPlaceHolderTransformOp.h>

namespace DB
{
String GeneratedColumnPlaceHolderTransformOp::getName() const
{
    return "GeneratedColumnPlaceholderTransformOp";
}

ReturnOpStatus GeneratedColumnPlaceHolderTransformOp::transformImpl(Block & block)
{
    action.transform(block);
    return OperatorStatus::HAS_OUTPUT;
}

void GeneratedColumnPlaceHolderTransformOp::transformHeaderImpl(Block & header_)
{
    header_ = action.getHeader();
}

void GeneratedColumnPlaceHolderTransformOp::operatePrefixImpl()
{
    action.checkColumn();
}
} // namespace DB
