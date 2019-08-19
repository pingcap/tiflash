#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class Context;

using Printer = std::function<void(const std::string &)>;

struct ClusterManage
{
    static void dumpRegionTable(Context & context, const ASTs & args, Printer output);
};
} // namespace DB
