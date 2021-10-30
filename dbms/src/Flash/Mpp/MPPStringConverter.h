#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Core/NamesAndTypes.h>

namespace DB
{
class Context;

class MPPStringConverter
{
public:
    MPPStringConverter(Context & context_, const tipb::DAGRequest & dag_request_);

    String buildMPPString();

private:
    Context & context;

    const tipb::DAGRequest & dag_request;
};

} // namespace DB