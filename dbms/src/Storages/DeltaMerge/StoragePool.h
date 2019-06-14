#pragma once

#include <atomic>

#include <Storages/Page/PageStorage.h>

namespace DB
{
class StoragePool
{
public:
    StoragePool(const String & path);

    PageId maxLogPageId() { return max_log_page_id; }
    PageId maxDataPageId() { return max_data_page_id; }
    PageId maxMetaPageId() { return max_meta_page_id; }

    PageId newLogPageId() { return ++max_log_page_id; }
    PageId newDataPageId() { return ++max_data_page_id; }
    PageId newMetaPageId() { return ++max_meta_page_id; }

    PageStorage & log() { return log_storage; }
    PageStorage & data() { return data_storage; }
    PageStorage & meta() { return meta_storage; }

private:
    std::atomic<PageId> max_log_page_id;
    std::atomic<PageId> max_data_page_id;
    std::atomic<PageId> max_meta_page_id;

    PageStorage log_storage;
    PageStorage data_storage;
    PageStorage meta_storage;
};
} // namespace DB