#pragma once

#include <tipb/select.pb.h>

namespace DB
{
class DAGExecutor
{
public:
    DAGExecutor(UInt32 id, const tipb::Executor & root);

    const tipb::Executor * source = nullptr;
    String source_name;
};
} // namespace DB
