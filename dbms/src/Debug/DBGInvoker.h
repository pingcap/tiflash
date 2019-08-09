#pragma once

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <Common/Exception.h>
#include <Parsers/IAST.h>

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
    BlockInputStreamPtr invokeSchemaless(Context & context, const std::string & name, const SchemalessDBGFunc & func, const ASTs & args);
    BlockInputStreamPtr invokeSchemaful(Context & context, const std::string & name, const SchemafulDBGFunc & func, const ASTs & args);

private:
    std::unordered_map<std::string, SchemalessDBGFunc> schemaless_funcs;
    std::unordered_map<std::string, SchemafulDBGFunc> schemaful_funcs;
};

} // namespace DB
