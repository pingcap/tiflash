#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <DataStreams/BlockIO.h>

namespace DB
{

class Context;

class DAGStringConverter
{
public:
    DAGStringConverter(Context & context_, const tipb::DAGRequest & dag_request_);

    ~DAGStringConverter() = default;

    String buildSqlString();

    const std::vector<NameAndTypePair> & getCurrentColumns()
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

protected:
    void buildTSString(const tipb::TableScan & ts, std::stringstream & ss);
    void buildSelString(const tipb::Selection & sel, std::stringstream & ss);
    void buildLimitString(const tipb::Limit & limit, std::stringstream & ss);
    void buildString(const tipb::Executor & executor, std::stringstream & ss);

protected:
    Context & context;
    const tipb::DAGRequest & dag_request;
    // used by columnRef, which starts with 1, and refs column index in the original ts/agg output
    std::vector<NameAndTypePair> columns_from_ts;
    std::vector<NameAndTypePair> columns_from_agg;
    // used by output_offset, which starts with 0, and refs the index in the selected output of ts/agg operater
    Names output_from_ts;
    Names output_from_agg;
    bool afterAgg;
};

} // namespace DB
