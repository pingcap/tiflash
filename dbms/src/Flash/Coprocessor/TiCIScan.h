#pragma once

#include <TiDB/Schema/TiDB.h>

#include "Flash/Coprocessor/DAGContext.h"
#include "tipb/executor.pb.h"
namespace DB
{
class DAGContext;

class TiCIScan
{
public:
    TiCIScan(const tipb::Executor * tici_scan_, const String & executor_id_, const DAGContext & dag_context);
    explicit TiCIScan(const tipb::Executor * tici_scan_);

    const TiDB::ColumnInfos & getQueryColumns() const { return query_columns; }
    const TiDB::ColumnInfos & getReturnColumns() const { return return_columns; }
    const int & getTableId() const { return table_id; }
    const int & getIndexId() const { return index_id; }
    const std::string & getQuery() const { return query_json_str; }
    const int & getLimit() const { return limit; }

private:
    const tipb::Executor * tici_scan;
    [[maybe_unused]] String executor_id;
    const int table_id;
    const int index_id;
    TiDB::ColumnInfos return_columns;
    TiDB::ColumnInfos query_columns;
    [[maybe_unused]] tipb::TiCIScanQueryType query_type;
    const std::string query_json_str;
    const int limit;
};
} // namespace DB
