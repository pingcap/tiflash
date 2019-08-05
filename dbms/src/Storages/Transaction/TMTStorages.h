#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include <Storages/Transaction/Types.h>

namespace DB
{

class StorageMergeTree;
using TMTStoragePtr = std::shared_ptr<StorageMergeTree>;

class TMTStorages : private boost::noncopyable
{
public:
    void put(TMTStoragePtr storage);

    TMTStoragePtr get(TableID table_id) const;
    std::unordered_map<TableID, TMTStoragePtr> getAllStorage() const;

    TMTStoragePtr getByName(const std::string & db, const std::string & table) const;

    void remove(TableID table_id);

private:
    std::unordered_map<TableID, TMTStoragePtr> storages;
    mutable std::mutex mutex;
};

} // namespace DB
