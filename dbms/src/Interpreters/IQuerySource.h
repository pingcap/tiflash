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

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST.h>

namespace DB
{
/// A tiny abstraction of different sources a query comes from, i.e. SQL string or DAG request.
class IQuerySource
{
public:
    virtual ~IQuerySource() = default;

    virtual std::tuple<std::string, ASTPtr> parse(size_t max_query_size) = 0;
    virtual String str(size_t max_query_size) = 0;
    virtual std::unique_ptr<IInterpreter> interpreter(Context & context, QueryProcessingStage::Enum stage) = 0;
};

} // namespace DB
