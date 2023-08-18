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

#include <Common/Exception.h>
#include <Parsers/IAST.h>

#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class Context;
class IBlockInputStream;

using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

class DBGInvoker
{
public:
    using Printer = std::function<void(const std::string &)>;
    using SchemalessDBGFunc = std::function<void(Context & context, const ASTs & args, Printer printer)>;
    using SchemafulDBGFunc = std::function<BlockInputStreamPtr(Context & context, const ASTs & args)>;

    DBGInvoker();

    void regSchemalessFunc(const std::string & name, SchemalessDBGFunc func) { schemaless_funcs[name] = func; }
    void regSchemafulFunc(const std::string & name, SchemafulDBGFunc func) { schemaful_funcs[name] = func; }

    BlockInputStreamPtr invoke(Context & context, const std::string & ori_name, const ASTs & args);
    static BlockInputStreamPtr invokeSchemaless(
        Context & context,
        const std::string & name,
        const SchemalessDBGFunc & func,
        const ASTs & args);
    static BlockInputStreamPtr invokeSchemaful(
        Context & context,
        const std::string & name,
        const SchemafulDBGFunc & func,
        const ASTs & args);

private:
    std::unordered_map<std::string, SchemalessDBGFunc> schemaless_funcs;
    std::unordered_map<std::string, SchemafulDBGFunc> schemaful_funcs;
};

} // namespace DB
