#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include <Storages/Transaction/Types.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class TMTStorages
{
public:
    void put(StoragePtr storage);

    StoragePtr get(TableID table_id) const;
    std::unordered_map<TableID, StoragePtr> getAllStorage() const;

    StoragePtr getByName(const std::string & db, const std::string & table) const;

    void remove(TableID table_id);

private:
    std::unordered_map<TableID, StoragePtr> storages;
    mutable std::mutex mutex;
};

} // namespace DB
