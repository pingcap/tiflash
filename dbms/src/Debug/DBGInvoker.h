#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include <Common/Exception.h>

#include <Interpreters/Context.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class DBGInvoker
{
public:
    using Printer = std::function<void(const std::string &)>;
    using DBGFunc = std::function<void(Context & context, const ASTs & args, Printer printer)>;

    DBGInvoker();

    void regFunc(const std::string & name, DBGFunc func)
    {
        funcs[name] = func;
    }

    BlockInputStreamPtr invoke(Context & context, const std::string & ori_name, const ASTs & args);

private:
    std::unordered_map<std::string, DBGFunc> funcs;
};

}
