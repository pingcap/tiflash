#pragma once

#include <tipb/select.pb.h>

namespace DB
{
class DAGContext;
class DAGRequestVerifier
{
public:
    explicit DAGRequestVerifier(const DAGContext & dag_context_)
        : dag_context(dag_context_)
    {}

    void verify(const tipb::DAGRequest * dag_request);

private:
    const DAGContext & dag_context;
};
} // namespace DB