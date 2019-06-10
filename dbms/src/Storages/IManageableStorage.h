#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
namespace DB
{
class IManageableStorage : public IStorage
{
public:
    explicit IManageableStorage(const ColumnsDescription & columns_) : IStorage(columns_) {}
    virtual ~IManageableStorage() = default;

    virtual void flushDelta() {}

    virtual BlockInputStreamPtr status() { return {}; }

    virtual void check(const Context &) {}
};

} // namespace DB