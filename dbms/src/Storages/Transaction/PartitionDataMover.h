#pragma once

#include <Interpreters/Context.h>

#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{


/// Remove range from this partition.
/// Note that [begin, excluded_end) is not necessarily to locate in the range of this partition (or table).
void deleteRange(const Context& context, StorageMergeTree* storage,
                 const HandleID begin, const HandleID excluded_end);

}
