#include "Flash/Coprocessor/TiCIScan.h"

#include "TiDB/Schema/TiDB.h"
namespace DB
{
TiCIScan::TiCIScan(const tipb::Executor * tici_scan_, const String & executor_id_, const DAGContext &)
    : tici_scan(tici_scan_)
    , executor_id(executor_id_)
    , table_id(tici_scan->tici_scan().table_id())
    , index_id(tici_scan->tici_scan().index_id())
    , return_columns(TiDB::toTiDBColumnInfos(tici_scan->tici_scan().return_columns()))
    , query_columns(TiDB::toTiDBColumnInfos(tici_scan->tici_scan().query_columns()))
    , query_type(tici_scan->tici_scan().type())
    , query_json_str(tici_scan->tici_scan().query_json_str())
    , limit(tici_scan->tici_scan().limit())
{}
} // namespace DB
