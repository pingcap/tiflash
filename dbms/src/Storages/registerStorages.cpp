#include <Common/config.h>
#include <Storages/StorageFactory.h>
#include <Storages/registerStorages.h>


namespace DB
{
void registerStorageLog(StorageFactory & factory);
void registerStorageTinyLog(StorageFactory & factory);
void registerStorageDeltaMerge(StorageFactory & factory);
void registerStorageStripeLog(StorageFactory & factory);
void registerStorageNull(StorageFactory & factory);
void registerStorageMerge(StorageFactory & factory);
void registerStorageBuffer(StorageFactory & factory);
void registerStorageMemory(StorageFactory & factory);
void registerStorageFile(StorageFactory & factory);
void registerStorageDictionary(StorageFactory & factory);
void registerStorageSet(StorageFactory & factory);
void registerStorageJoin(StorageFactory & factory);
void registerStorageView(StorageFactory & factory);
void registerStorageMaterializedView(StorageFactory & factory);


void registerStorages()
{
    auto & factory = StorageFactory::instance();

    registerStorageLog(factory);
    registerStorageTinyLog(factory);
    registerStorageDeltaMerge(factory);
    registerStorageStripeLog(factory);
    registerStorageNull(factory);
    registerStorageMerge(factory);
    registerStorageBuffer(factory);
    registerStorageMemory(factory);
    registerStorageFile(factory);
    registerStorageDictionary(factory);
    registerStorageSet(factory);
    registerStorageJoin(factory);
    registerStorageView(factory);
    registerStorageMaterializedView(factory);
}

} // namespace DB
