#pragma once
#include <memory>

namespace DB
{
class PageStorageSnapshot
{
public:
    virtual ~PageStorageSnapshot() = default;
};
using PageStorageSnapshotPtr = std::shared_ptr<PageStorageSnapshot>;

} // namespace DB
