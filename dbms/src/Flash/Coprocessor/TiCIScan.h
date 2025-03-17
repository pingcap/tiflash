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

    const TiDB::ColumnInfos & getColumns() const { return columns; }

private:
    [[maybe_unused]] const tipb::Executor * tici_scan;
    [[maybe_unused]] String executor_id;
    [[maybe_unused]] int table_id;
    [[maybe_unused]] int index_id;
    [[maybe_unused]] const TiDB::ColumnInfos columns;
    [[maybe_unused]] tipb::TiCIScanQueryType query_type;
    [[maybe_unused]] std::string query_json_str;
    [[maybe_unused]] int limit;
};
} // namespace DB
