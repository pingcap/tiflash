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
    //   ./storages-client.sh "DBGInvoke find_region_by_range(start_key, end_key)"
    static void findRegionByRange(Context & context, const ASTs & args, Printer output);

    static void checkTableOptimize(Context & context, const ASTs & args, Printer output);
};
} // namespace DB
