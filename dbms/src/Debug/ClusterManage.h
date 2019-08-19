#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class Context;

using Printer = std::function<void(const std::string &)>;

struct ClusterManage
{
    // Reset schemas.
    // Usage:
    //   ./storages-client.sh "DBGInvoke dump_region_table(table_id, if_dump_regions)"
    static void dumpRegionTable(Context & context, const ASTs & args, Printer output);
};
} // namespace DB
