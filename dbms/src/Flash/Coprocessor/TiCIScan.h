#pragma once

#include <TiDB/Schema/TiDB.h>

#include "Flash/Coprocessor/DAGContext.h"
#include "tipb/executor.pb.h"
namespace DB
{
class DAGContext;

const int test_limit = 10;
const std::string test_query = "sea";

class TiCIScan
{
public:
    TiCIScan(const tipb::Executor * tici_scan_, const String & executor_id_, const DAGContext & dag_context);

    const TiDB::ColumnInfos & getColumns() const { return columns; }
    const int & getTableId() const { return table_id; }
    const std::string & getQuery() const { return query_json_str; }
    const int & getLimit() const { return limit; }

private:
    [[maybe_unused]] const tipb::Executor * tici_scan;
    [[maybe_unused]] String executor_id;
    const int table_id;
    [[maybe_unused]] int index_id;
    const TiDB::ColumnInfos columns;
    [[maybe_unused]] tipb::TiCIScanQueryType query_type;
    const std::string query_json_str;
    const int limit;
};
} // namespace DB
