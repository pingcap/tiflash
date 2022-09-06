// Copyright 2022 PingCAP, Ltd.
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

#include <Columns/FilterDescription.h>
#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Transforms/Transforms.h>
#include <common/types.h>

namespace DB
{
class FilterTransform : public Transform
{
public:
    FilterTransform(
        const Block & input_header,
        const ExpressionActionsPtr & expression_,
        const String & filter_column_name);

    bool transform(Block & block) override;

    void transformHeader(Block & block) override;

private:
    ExpressionActionsPtr expression;
    Block header;
    size_t filter_column;

    ConstantFilterDescription constant_filter_description;
};
} // namespace DB
