#pragma once

namespace DB
{

class TMTStorages
{
public:
    void put(StoragePtr storage)
    {
        std::lock_guard lock(mutex);

        const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get());
        if (!merge_tree)
            throw Exception{storage->getName() + " is not in MergeTree family", ErrorCodes::LOGICAL_ERROR};

        TableID table_id = merge_tree->getTableInfo().id;
        if (storages.find(table_id) != storages.end())
            return;
        storages.emplace(table_id, storage);
    }

    StoragePtr get(TableID table_id)
    {
        std::lock_guard lock(mutex);

        auto it = storages.find(table_id);
        if (it == storages.end())
            return nullptr;
        return it->second;
    }

    void remove(TableID table_id)
    {
        std::lock_guard lock(mutex);

        auto it = storages.find(table_id);
        if (it == storages.end())
            return;
        storages.erase(it);
    }

private:
    std::unordered_map<TableID, StoragePtr> storages;
    std::mutex mutex;
};

}
