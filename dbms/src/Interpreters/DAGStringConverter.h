#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Coprocessor/CoprocessorHandler.h>
#include <DataStreams/BlockIO.h>

namespace DB
{

class DAGStringConverter
{
public:
    DAGStringConverter(CoprocessorContext & context_, tipb::DAGRequest & dag_request_);

    ~DAGStringConverter() = default;

    String buildSqlString();

private:
    bool buildTSString(const tipb::TableScan & ts, std::stringstream & ss);
    bool buildSelString(const tipb::Selection & sel, std::stringstream & ss);
    bool buildLimitString(const tipb::Limit & limit, std::stringstream & ss);
    bool buildString(const tipb::Executor & executor, std::stringstream & ss);
    CoprocessorContext & context;
    tipb::DAGRequest & dag_request;
    // used by columnRef, which starts with 1, and refs column index in the original ts/agg output
    NamesAndTypesList columns_from_ts;
    NamesAndTypesList columns_from_agg;
    // used by output_offset, which starts with 0, and refs the index in the selected output of ts/agg operater
    Names output_from_ts;
    Names output_from_agg;
    bool afterAgg;
    const NamesAndTypesList & getCurrentColumns()
    {
        if (afterAgg)
        {
            return columns_from_agg;
        }
        return columns_from_ts;
    }

    const Names & getCurrentOutputColumns()
    {
        if (afterAgg)
        {
            return output_from_agg;
        }
        return output_from_ts;
    }
};

} // namespace DB
