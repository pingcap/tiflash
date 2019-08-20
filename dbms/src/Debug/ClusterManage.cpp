#include <Common/typeid_cast.h>
#include <Debug/ClusterManage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_TABLE;
} // namespace ErrorCodes

void ClusterManage::dumpRegionTable(Context & context, const ASTs & args, Printer output)
{
    if (args.size() < 2)
        throw Exception("Args not matched, should be: table-id, if-dump-regions", ErrorCodes::BAD_ARGUMENTS);

    auto & tmt = context.getTMTContext();
    TableID table_id = (TableID)safeGet<UInt64>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    bool dump_regions = typeid_cast<const ASTIdentifier &>(*args[1]).name == "true";

    RegionTable::InternalRegions regions;
    size_t count = 0;

    tmt.getRegionTable().dumpRegionsByTable(table_id, count, dump_regions ? &regions : nullptr);

    output(toString(count));
    if (dump_regions)
    {
        std::stringstream ss;

        for (const auto & region : regions)
        {
            ss << region.first << ',' << region.second.cache_bytes << ' ';
        }
        output(ss.str());
    }
}

} // namespace DB
