#include "Flash/Coprocessor/TiCIScan.h"
namespace DB
{
TiCIScan::TiCIScan(const tipb::Executor * tici_scan_, const String & executor_id_, const DAGContext &)
    : tici_scan(tici_scan_)
    , executor_id(executor_id_)
    //, table_id(tici_scan->tici_scan().table_id())
    //, index_id(tici_scan->tici_scan().index_id())
    //, columns(TiDB::toTiDBColumnInfos(tici_scan->tici_scan().columns()))
    //, query_type(tici_scan->tici_scan().type())
    //, query_json_str(tici_scan->tici_scan().query_json_str())
    //, limit(tici_scan->tici_scan().limit())

    , table_id(tici_scan_->tbl_scan().table_id())
    , index_id(0)
    , columns(TiDB::toTiDBColumnInfos(tici_scan->tbl_scan().columns()))
    , query_type(tipb::TiCIScanQueryType::QueryMatch)
    , query_json_str(test_query)
    , limit(test_limit)
{}

} // namespace DB
