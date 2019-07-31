#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>
#include <Coprocessor/CoprocessorHandler.h>

namespace DB {

class DAGStringConverter {
public:
    DAGStringConverter(CoprocessorContext & context_, tipb::DAGRequest & dag_request_);

    ~DAGStringConverter() = default;

    String buildSqlString();
private:
    bool buildTSString(const tipb::TableScan & ts, std::stringstream & ss);
    String exprToString(const tipb::Expr & expr, bool &succ);
    bool buildSelString(const tipb::Selection & sel, std::stringstream & ss);
    bool buildLimitString(const tipb::Limit & limit, std::stringstream & ss);
    bool buildString(const tipb::Executor & executor, std::stringstream & ss);
    CoprocessorContext & context;
    tipb::DAGRequest & dag_request;
    std::unordered_map<Int64, std::string> column_name_from_ts;
    std::unordered_map<Int64, std::string> column_name_from_agg;
    bool afterAgg;
    std::unordered_map<Int64, std::string> & getCurrentColumnNames() {
        if(afterAgg) {
            return column_name_from_agg;
        }
        return column_name_from_ts;
    }

};

}
