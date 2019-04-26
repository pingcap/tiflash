#pragma once

#include <functional>
#include <memory>

namespace enginepb
{
class SnapshotRequest;
}

namespace DB
{

class Context;
class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

// Simplify test.
using RequestReader = std::function<bool(enginepb::SnapshotRequest *)>;

void applySnapshot(KVStorePtr kvstore, RequestReader read, Context * context = nullptr);

} // namespace DB
