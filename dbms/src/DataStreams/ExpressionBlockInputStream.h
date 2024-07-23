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

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{
class ExpressionActions;

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%yandex%'
  * The expression processes each row independently of the others.
  */
class ExpressionBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
    static constexpr auto NAME = "Expression";

public:
    ExpressionBlockInputStream(
        const BlockInputStreamPtr & input,
        const ExpressionActionsPtr & expression_,
        const String & req_id);

    String getName() const override { return NAME; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ExpressionActionsPtr expression;
    const LoggerPtr log;
};

} // namespace DB
