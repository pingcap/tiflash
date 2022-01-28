#pragma once

#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
std::vector<tipb::FieldType> collectOutputFieldTypes(const tipb::DAGRequest & dag_request);
}