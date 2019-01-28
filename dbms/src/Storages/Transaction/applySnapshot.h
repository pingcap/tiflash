#pragma once

#include <Storages/Transaction/KVStore.h>

namespace DB
{

// Simplify test.
using RequestReader = std::function<bool(enginepb::SnapshotRequest *)>;

void applySnapshot(KVStorePtr kvstore, RequestReader read, Context * context = nullptr);

} // namespace DB
